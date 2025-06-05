package pubsubmutex

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Global package-level flag for debug logging
var debugEnabled bool

// SetDebug enables or disables debug logging for the pubsub package.
func SetDebug(enable bool) {
	debugEnabled = enable
}

// logDebug prints a debug message if debug logging is enabled.
// This is an internal helper and not part of the public API.
func logDebug(format string, a ...interface{}) {
	if debugEnabled {
		fmt.Printf("[PUBSUB DEBUG] "+format+"\n", a...)
	}
}

// uniqueIDCounter is used to generate unique subscriber IDs.
var uniqueIDCounter atomic.Uint64

// GetUniqueSubscriberID generates a new unique subscriber ID.
// Although it's a method on PubSub, it currently uses a global atomic counter,
// ensuring IDs are unique across all PubSub instances if this package-level variable is shared.
func (ps *PubSub) GetUniqueSubscriberID() string {
	return fmt.Sprintf("sub-%d", uniqueIDCounter.Add(1))
}

// Message represents a message to be published.
// It contains the topic the message is for and the actual data.
type Message struct {
	Topic string      // The topic this message belongs to.
	Data  interface{} // The payload of the message.
}

// DefaultPublishTimeout is the default duration a publisher will wait
// for a message to be accepted by a subscriber's internal buffer if
// the topic is configured for blocking sends (AllowDropping=false)
// and no specific TopicConfig overrides this for the topic.
const DefaultPublishTimeout = 500 * time.Millisecond

// TopicConfig allows configuring behavior for a specific topic.
type TopicConfig struct {
	// AllowDropping, if true, means messages published to this topic
	// might be dropped if a subscriber's internal buffer is full.
	// If false (default), publishing will block until the message is accepted
	// by the subscriber's internal buffer or a timeout occurs.
	AllowDropping bool

	// PublishTimeout specifies the maximum time a publisher will wait
	// for a message to be accepted by a subscriber's internal buffer
	// if AllowDropping is false.
	// A value of 0 means no timeout (publisher will block indefinitely).
	// If not set for a topic, DefaultPublishTimeout is used for blocking publishes.
	PublishTimeout time.Duration
}

// Subscriber represents a client subscribed to a topic.
// It provides a channel (Ch) for receiving messages and can manage its own unsubscription.
type Subscriber struct {
	ID                string         // Unique identifier for the subscriber.
	Topic             string         // The topic this subscriber is registered to.
	Ch                chan Message   // Public channel for the client to receive messages.
	internalCh        chan Message   // Internal buffered channel where messages are first queued.
	close             chan struct{}  // Signals the internal delivery goroutine to shut down.
	shutdownOnce      sync.Once      // Ensures the shutdown signal (closing s.close) happens only once.
	deliveryWg        sync.WaitGroup // Waits for the message delivery goroutine to complete.
	internalCloseOnce sync.Once      // Ensures s.internalCh is closed only once.

	// unsubscribeFunc is a callback provided by PubSub to initiate this subscriber's cleanup.
	unsubscribeFunc func()
	// unsubscribed tracks if Unsubscribe has been called, for idempotency.
	unsubscribed atomic.Bool
}

// PubSub represents the core publish-subscribe system.
// It manages topics, subscribers, and message dispatching.
type PubSub struct {
	mu           sync.RWMutex                      // Protects access to subscribers and topicConfigs.
	subscribers  map[string]map[string]*Subscriber // Map of topic to (map of subscriberID to Subscriber).
	topicConfigs map[string]TopicConfig            // Map of topic to its specific configuration.
}

// NewPubSub creates and returns a new PubSub system instance.
func NewPubSub() *PubSub {
	ps := &PubSub{
		subscribers:  make(map[string]map[string]*Subscriber),
		topicConfigs: make(map[string]TopicConfig),
	}
	logDebug("New PubSub system created.")
	return ps
}

// CreateTopic explicitly creates and configures a topic with specific behaviors.
// If a topic is published to without being explicitly created, it will use default configurations.
func (ps *PubSub) CreateTopic(topic string, config TopicConfig) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.topicConfigs[topic] = config
	logDebug("Topic '%s' created with config: %+v", topic, config)
}

// getTopicConfig retrieves the configuration for a given topic.
// If no specific config is set, it returns a default configuration.
// This is an unexported helper method.
func (ps *PubSub) getTopicConfig(topic string) TopicConfig {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if config, ok := ps.topicConfigs[topic]; ok {
		return config
	}
	return TopicConfig{AllowDropping: false, PublishTimeout: DefaultPublishTimeout}
}

// Subscribe allows a client to register for messages on a specific topic.
// It returns a new Subscriber instance. The subscriberID must be unique for the given topic.
// bufferSize determines the capacity of the internal message buffer for this subscriber;
// if messages arrive faster than the client consumes them from Subscriber.Ch, they will
// queue up in this internal buffer up to its capacity.
func (ps *PubSub) Subscribe(topic string, subscriberID string, bufferSize int) *Subscriber {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if bufferSize < 0 {
		logDebug("Subscribe: bufferSize %d for '%s' on topic '%s' is negative, defaulting to 0.", bufferSize, subscriberID, topic)
		bufferSize = 0
	}

	var newTopicMapCreated bool
	if _, ok := ps.subscribers[topic]; !ok {
		ps.subscribers[topic] = make(map[string]*Subscriber)
		newTopicMapCreated = true
	}

	if _, exists := ps.subscribers[topic][subscriberID]; exists {
		logDebug("Error: Subscriber '%s' already exists for topic '%s'. Returning nil.", subscriberID, topic)
		return nil
	}

	sub := &Subscriber{
		ID:         subscriberID,
		Topic:      topic,
		Ch:         make(chan Message),
		internalCh: make(chan Message, bufferSize),
		close:      make(chan struct{}),
	}

	// Assign the self-unsubscription callback.
	// This closure captures 'ps' and 'sub' for use in Subscriber.Unsubscribe().
	sub.unsubscribeFunc = func() {
		logDebug("Subscriber %s (topic '%s') initiated self-unsubscription via unsubscribeFunc.", sub.ID, sub.Topic)
		ps.CleanupSub(sub) // Call the main PubSub cleanup logic for this subscriber.
	}

	ps.subscribers[topic][subscriberID] = sub
	sub.deliveryWg.Add(1)
	go sub.deliverMessages()

	if newTopicMapCreated {
		logDebug("Created new topic map for '%s'.", topic)
	}
	logDebug("Subscriber '%s' subscribed to topic '%s' with buffer size %d.", subscriberID, topic, bufferSize)
	return sub
}

// Unsubscribe signals the PubSub system to remove and clean up this subscriber instance.
// It ensures that the subscriber's message channel (s.Ch) will eventually be closed
// and its internal delivery goroutine will terminate.
// This method is idempotent; calling it multiple times on the same subscriber
// will only perform the unsubscription process once.
// The call blocks until the subscriber's cleanup is complete.
func (s *Subscriber) Unsubscribe() {
	if s.unsubscribed.CompareAndSwap(false, true) {
		if s.unsubscribeFunc != nil {
			logDebug("Subscriber %s (topic '%s'): Calling unsubscribeFunc.", s.ID, s.Topic)
			s.unsubscribeFunc()
		} else {
			// This might happen if Unsubscribe is called on a detached or improperly initialized Subscriber.
			logDebug("Subscriber %s (topic '%s'): Unsubscribe called but unsubscribeFunc is nil.", s.ID, s.Topic)
		}
	} else {
		logDebug("Subscriber %s (topic '%s'): Unsubscribe called but already in process or completed.", s.ID, s.Topic)
	}
}

// deliverMessages is an internal goroutine run for each subscriber.
// It reads messages from the subscriber's internalCh and sends them to the public Ch.
// It also handles graceful shutdown when signaled via the s.close channel.
func (s *Subscriber) deliverMessages() {
	defer s.deliveryWg.Done()
	defer func() {
		// Ensure the public channel is always closed when this goroutine exits.
		close(s.Ch)
		logDebug("Subscriber %s (topic '%s') public channel s.Ch closed.", s.ID, s.Topic)
	}()

	logDebug("Subscriber %s (topic '%s') delivery goroutine started.", s.ID, s.Topic)
	for {
		select {
		case msg, ok := <-s.internalCh:
			if !ok { // internalCh was closed (likely by the 'case <-s.close:' path below).
				logDebug("Subscriber %s (topic '%s') delivery: internalCh closed. Exiting.", s.ID, s.Topic)
				return
			}

			logDebug("Subscriber %s (topic '%s') delivery: attempting to send to s.Ch: %v", s.ID, s.Topic, msg.Data)
			// This select ensures that if s.Ch is blocked during shutdown (s.close is closed),
			// we can drop the current message and continue processing to allow deliverMessages to terminate.
			select {
			case s.Ch <- msg:
				logDebug("Subscriber %s (topic '%s') delivery: sent to s.Ch: %v", s.ID, s.Topic, msg.Data)
			case <-s.close: // If s.close is already closed, this path is non-blocking.
				logDebug("Subscriber %s (topic '%s') delivery: s.close signal received while sending to s.Ch. Message dropped: %v", s.ID, s.Topic, msg.Data)
				// The main 'case <-s.close:' path is responsible for initiating internalCh closure.
				// This path just ensures we don't block indefinitely on s.Ch during shutdown.
			}

		case <-s.close: // External shutdown signal (from CleanupSub or Subscriber.Unsubscribe).
			logDebug("Subscriber %s (topic '%s') delivery: s.close signal received. Initiating internalCh closure.", s.ID, s.Topic)
			s.internalCloseOnce.Do(func() {
				close(s.internalCh)
			})
			// The loop continues. The 'case msg, ok := <-s.internalCh:' will then read remaining messages
			// from the now-closing internalCh until it's drained, then `ok` will be false, and the goroutine will return.
		}
	}
}

// CleanupSub performs the actual unsubscription and cleanup of a subscriber.
// It removes the subscriber from the PubSub's internal tracking, signals the
// subscriber's message delivery goroutine to shut down, facilitates draining its
// public message channel, and waits for the delivery goroutine to terminate.
// This function is primarily called internally by Subscriber.Unsubscribe or PubSub.Close.
func (ps *PubSub) CleanupSub(sub *Subscriber) {
	if sub == nil {
		logDebug("CleanupSub called with nil subscriber.")
		return
	}

	// Capture details for logging before map removal, as 'sub' might be from an external call.
	originalSubID := sub.ID
	originalSubTopic := sub.Topic
	var instanceRemovedFromMap bool

	// Scoped lock for map manipulation.
	func() {
		ps.mu.Lock()
		defer ps.mu.Unlock()

		topicSubscribers, topicExists := ps.subscribers[originalSubTopic]
		if !topicExists {
			logDebug("CleanupSub: Topic '%s' not found for subscriber '%s'.", originalSubTopic, originalSubID)
			return // instanceRemovedFromMap remains false
		}

		existingSubInMap, subExistsInMap := topicSubscribers[originalSubID]
		if !subExistsInMap {
			logDebug("CleanupSub: Subscriber ID '%s' not found in topic '%s'.", originalSubID, originalSubTopic)
			return // instanceRemovedFromMap remains false
		}
		// Important: only remove from map if the instance matches the one we were given.
		// This prevents issues if CleanupSub is called with a stale 'sub' pointer after
		// a new subscriber with the same ID (if IDs were not unique) was created.
		// With unique IDs and the current test structure, this check is mostly for extreme robustness.
		if existingSubInMap != sub {
			logDebug("CleanupSub: Subscriber instance mismatch for ID '%s' in topic '%s'. Given %p, map has %p. Not removing from map.", originalSubID, originalSubTopic, sub, existingSubInMap)
			return // instanceRemovedFromMap remains false
		}

		logDebug("CleanupSub: Removing subscriber '%s' from topic '%s'.", originalSubID, originalSubTopic)
		delete(topicSubscribers, originalSubID)
		if len(topicSubscribers) == 0 {
			logDebug("CleanupSub: Topic '%s' has no more subscribers, removing topic entry.", originalSubTopic)
			delete(ps.subscribers, originalSubTopic)
		}
		instanceRemovedFromMap = true
	}()

	if !instanceRemovedFromMap {
		logDebug("CleanupSub: Subscriber '%s' (topic '%s') was not found in map for removal or was a different instance. Proceeding with shutdown signal for passed instance.", originalSubID, originalSubTopic)
	}

	logDebug("CleanupSub: Initiating shutdown signal for subscriber '%s' (topic '%s').", sub.ID, sub.Topic)
	sub.shutdownOnce.Do(func() { // Ensures close(sub.close) is called at most once.
		close(sub.close)
		logDebug("CleanupSub: Signaled close (via shutdownOnce) for subscriber '%s'.", sub.ID)
	})

	// Goroutine to drain the subscriber's public channel (s.Ch).
	// This is crucial to allow the deliverMessages goroutine to send any final messages
	// to s.Ch and terminate, even if the client code is no longer reading from s.Ch.
	go func(s *Subscriber) {
		logDebug("Subscriber '%s' (topic '%s') - CleanupSub: starting drainer for public channel s.Ch.", s.ID, s.Topic)
		for range s.Ch { /* Drain messages until s.Ch is closed by deliverMessages */
		}
		logDebug("Subscriber '%s' (topic '%s') - CleanupSub: finished draining public channel s.Ch.", s.ID, s.Topic)
	}(sub)

	sub.deliveryWg.Wait() // Wait for the deliverMessages goroutine to fully complete.
	logDebug("Subscriber '%s' (topic '%s') cleanup complete.", sub.ID, sub.Topic)
}

// SendReceive subscribes to a receive topic, publishes a message to a send topic,
// and waits for a response on the receive topic or until the specified timeout.
// It returns the data from the received message or nil if timed out or an error occurred.
func (ps *PubSub) SendReceive(sendTopic string, receiveTopic string, sendMsg interface{}, timeoutMs int) (result interface{}) {
	subID := ps.GetUniqueSubscriberID()
	// Buffer of 1 is usually sufficient as we expect at most one response.
	sub := ps.Subscribe(receiveTopic, subID, 1)
	if sub == nil {
		logDebug("SendReceive: Failed to subscribe to receive topic '%s'.", receiveTopic)
		return nil
	}
	defer sub.Unsubscribe() // Use the new Subscriber.Unsubscribe method for cleanup.

	ps.Publish(Message{Topic: sendTopic, Data: sendMsg})

	select {
	case msg, ok := <-sub.Ch:
		if ok {
			result = msg.Data
		} else {
			// Channel was closed before a message was received.
			logDebug("SendReceive: Subscriber channel for topic '%s' closed before message received.", receiveTopic)
		}
	case <-time.After(time.Millisecond * time.Duration(timeoutMs)):
		logDebug("SendReceive timeout of %dms reached for receive topic '%s'.", timeoutMs, receiveTopic)
	}
	return
}

// Publish sends a message to all relevant subscribers of the message's topic.
// Message delivery behavior (dropping vs. blocking with timeout) depends on the
// topic's configuration (see TopicConfig and CreateTopic).
func (ps *PubSub) Publish(message Message) {
	topicConfig := ps.getTopicConfig(message.Topic) // getTopicConfig handles its own RLock.
	ps.mu.RLock()
	var subsToNotify []*Subscriber
	if topicSubs, ok := ps.subscribers[message.Topic]; ok {
		subsToNotify = make([]*Subscriber, 0, len(topicSubs))
		for _, sub := range topicSubs {
			subsToNotify = append(subsToNotify, sub)
		}
	}
	ps.mu.RUnlock() // Release lock before spawning goroutines and potentially blocking sends.

	if len(subsToNotify) == 0 {
		// logDebug("No subscribers found for topic '%s'. Message not delivered.", message.Topic) // Can be noisy
		return
	}

	logDebug("Publishing message to topic '%s' to %d subscribers: %v", message.Topic, len(subsToNotify), message.Data)

	var wg sync.WaitGroup
	for _, currentSub := range subsToNotify {
		wg.Add(1)
		go func(s *Subscriber, m Message, tc TopicConfig) {
			defer wg.Done() // Ensure WaitGroup is always decremented.

			// Anonymous function to scope panic recovery for the send logic.
			func() {
				defer func() {
					if r := recover(); r != nil {
						// Log panic (e.g., send on closed s.internalCh) but don't crash PubSub.
						logDebug("PANIC recovered in Publish send logic for sub %s (topic %s): %v. Message data: %v. Dropped.", s.ID, s.Topic, r, m.Data)
					}
				}()

				// Initial non-blocking check: if subscriber is already closing, don't attempt to send.
				select {
				case <-s.close:
					logDebug("Warning: Not sending to subscriber '%s' (topic '%s') as it's closing (initial check).", s.ID, s.Topic)
					return // Exit anonymous function; outer defer wg.Done() will run.
				default:
					// Subscriber not immediately detected as closing, proceed to send attempt.
				}

				if tc.AllowDropping {
					// For dropping, prefer to check s.close again to avoid sending to a closing sub.
					select {
					case <-s.close:
						logDebug("Publish/Drop: Subscriber '%s' closed before non-blocking send attempt.", s.ID)
					default:
						// s.close not immediately readable, try non-blocking send to internalCh.
						select {
						case s.internalCh <- m:
							logDebug("Message delivered (non-blocking) to internal channel for subscriber '%s'.", s.ID)
						default: // Channel full or (if send to closed panics) recovered by outer defer.
							logDebug("Warning: Dropping message for subscriber '%s' (channel full or closing, dropping allowed).", s.ID)
						}
					}
				} else { // Not AllowDropping (blocking send with timeout).
					var C_timeoutChan <-chan time.Time
					var timer *time.Timer
					if tc.PublishTimeout > 0 {
						timer = time.NewTimer(tc.PublishTimeout)
						C_timeoutChan = timer.C
						defer timer.Stop() // Important: release timer resources if not fired.
					}

					select {
					case <-s.close: // Prioritize checking close signal.
						logDebug("Publish/Block: Subscriber '%s' closed. Not sending.", s.ID, s.Topic)
					case <-C_timeoutChan: // Handles nil timeoutChan correctly (case never selected if nil).
						logDebug("Publish/Block: Timeout (%s) for subscriber '%s' on topic '%s'. Message not delivered.", tc.PublishTimeout, s.ID, s.Topic)
					case s.internalCh <- m: // This send could panic if s.internalCh closes; recover will catch.
						logDebug("Message delivered (blocking) to internal channel for subscriber '%s'.", s.ID)
					}
				}
			}() // End of anonymous function call for panic recovery.
		}(currentSub, message, topicConfig)
	}
	wg.Wait() // Wait for all send goroutines to complete or be handled.
	logDebug("All internal sends for topic '%s' completed (or dropped/timed out/panicked internally).", message.Topic)
}

// Close gracefully shuts down the PubSub system.
// It unsubscribes all active subscribers and ensures their resources are released.
// Subsequent calls to Publish will find no subscribers. Subscribe calls will work but may be cleaned up if Close is called again.
func (ps *PubSub) Close() {
	logDebug("Initiating graceful shutdown of PubSub system.")

	// Safely get all current subscribers and clear the main maps under lock.
	allSubscribers := func() []*Subscriber {
		ps.mu.Lock()
		defer ps.mu.Unlock() // Ensures lock release for this scope.

		subs := make([]*Subscriber, 0)
		for _, topicSubs := range ps.subscribers {
			for _, sub := range topicSubs {
				subs = append(subs, sub)
			}
		}
		// Clear internal state.
		ps.subscribers = make(map[string]map[string]*Subscriber)
		ps.topicConfigs = make(map[string]TopicConfig)
		logDebug("PubSub subscribers and topicConfigs maps cleared.")
		return subs
	}() // Execute anonymous function.

	// ps.mu is now UNLOCKED. Proceed to clean up subscribers concurrently.

	var wg sync.WaitGroup // To wait for all individual subscriber cleanups.
	logDebug("PubSub.Close: Cleaning up %d subscribers.", len(allSubscribers))
	for _, subInstance := range allSubscribers {
		wg.Add(1)
		go func(s *Subscriber) {
			defer wg.Done()
			logDebug("Closing subscriber '%s' for topic '%s' during PubSub Close.", s.ID, s.Topic)
			// Use Subscriber.Unsubscribe which internally calls CleanupSub.
			// This ensures the same cleanup logic and idempotency is used.
			s.Unsubscribe()
			// The Unsubscribe call itself blocks until that specific subscriber's cleanup is done.
			logDebug("Subscriber '%s' (topic '%s') Unsubscribe call completed during PubSub Close.", s.ID, s.Topic)
		}(subInstance)
	}
	wg.Wait() // Wait for all subscriber cleanup goroutines (triggered by Unsubscribe) to complete.
	logDebug("PubSub system closed gracefully.")
}

// ReadMessages is a helper function for clients to continuously read messages
// from a subscriber's channel using a provided handler function.
// This function will block and run in the calling goroutine until the subscriber's
// channel (s.Ch) is closed (typically when the subscriber is unsubscribed).
func (s *Subscriber) ReadMessages(handler func(Message)) {
	logDebug("Subscriber %s (topic '%s') ReadMessages loop started.", s.ID, s.Topic)
	for msg := range s.Ch { // This loop exits when s.Ch is closed.
		logDebug("Subscriber %s (topic '%s') ReadMessages calling handler for: %v", s.ID, s.Topic, msg.Data)
		handler(msg)
	}
	logDebug("Subscriber %s (topic '%s') ReadMessages loop exited (s.Ch closed).", s.ID, s.Topic)
}
