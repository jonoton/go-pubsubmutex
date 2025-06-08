package pubsubmutex

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Cleanable is an interface for objects that need explicit resource management.
// If a message's Data field implements this interface, its Cleanup() method
// will be called automatically when the message is dropped (e.g., due to a full
// buffer, a publish timeout, or a closing subscriber). This helps prevent
// resource leaks for data that isn't delivered.
type Cleanable interface {
	Cleanup()
}

// Global package-level flag for debug logging
var debugEnabled bool

// SetDebug enables or disables debug logging for the pubsub package.
func SetDebug(enable bool) {
	debugEnabled = enable
}

// logDebug prints a debug message if debug logging is enabled.
func logDebug(format string, a ...interface{}) {
	if debugEnabled {
		fmt.Printf("[PUBSUB DEBUG] "+format+"\n", a...)
	}
}

// uniqueIDCounter is used to generate unique subscriber IDs.
var uniqueIDCounter atomic.Uint64

// GetUniqueSubscriberID generates a new unique subscriber ID.
func (ps *PubSub) GetUniqueSubscriberID() string {
	return fmt.Sprintf("sub-%d", uniqueIDCounter.Add(1))
}

// Message represents a message to be published. It contains the topic the
// message is for and the actual data.
type Message struct {
	Topic string
	Data  interface{}
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
	// if AllowDropping is false. A value of 0 means no timeout (publisher
	// will block indefinitely).
	PublishTimeout time.Duration
}

// Subscriber represents a client subscribed to a topic. It provides a channel (Ch)
// for receiving messages and can manage its own unsubscription.
type Subscriber struct {
	ID                string         // Unique identifier for the subscriber.
	Topic             string         // The topic this subscriber is registered to.
	Ch                chan Message   // Public channel for the client to receive messages.
	internalCh        chan Message   // Internal buffered channel where messages are first queued.
	close             chan struct{}  // Signals the internal delivery goroutine to shut down.
	shutdownOnce      sync.Once      // Ensures the shutdown signal (closing s.close) happens only once.
	deliveryWg        sync.WaitGroup // Waits for the message delivery goroutine to complete.
	internalCloseOnce sync.Once      // Ensures s.internalCh is closed only once.
	unsubscribeFunc   func()         // Callback provided by PubSub to initiate this subscriber's cleanup.
	unsubscribed      atomic.Bool    // Tracks if Unsubscribe has been called, for idempotency.
}

// PubSub represents the core publish-subscribe system. It manages topics,
// subscribers, and message dispatching.
type PubSub struct {
	mu           sync.RWMutex
	subscribers  map[string]map[string]*Subscriber
	topicConfigs map[string]TopicConfig
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
// bufferSize determines the capacity of the internal message buffer for this subscriber.
func (ps *PubSub) Subscribe(topic string, subscriberID string, bufferSize int) *Subscriber {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if bufferSize < 0 {
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

	sub.unsubscribeFunc = func() {
		logDebug("Subscriber %s (topic '%s') initiated self-unsubscription via unsubscribeFunc.", sub.ID, sub.Topic)
		ps.CleanupSub(sub)
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
// The call blocks until the subscriber's cleanup is complete. It is safe to call multiple times.
func (s *Subscriber) Unsubscribe() {
	if s.unsubscribed.CompareAndSwap(false, true) {
		if s.unsubscribeFunc != nil {
			logDebug("Subscriber %s (topic '%s'): Calling unsubscribeFunc.", s.ID, s.Topic)
			s.unsubscribeFunc()
		} else {
			logDebug("Subscriber %s (topic '%s'): Unsubscribe called but unsubscribeFunc is nil.", s.ID, s.Topic)
		}
	} else {
		logDebug("Subscriber %s (topic '%s'): Unsubscribe called but already in process or completed.", s.ID, s.Topic)
	}
}

// deliverMessages is an internal goroutine run for each subscriber.
func (s *Subscriber) deliverMessages() {
	defer s.deliveryWg.Done()
	defer func() {
		close(s.Ch)
		logDebug("Subscriber %s (topic '%s') public channel s.Ch closed.", s.ID, s.Topic)
	}()

	logDebug("Subscriber %s (topic '%s') delivery goroutine started.", s.ID, s.Topic)
	for {
		select {
		case msg, ok := <-s.internalCh:
			if !ok {
				logDebug("Subscriber %s (topic '%s') delivery: internalCh closed. Exiting.", s.ID, s.Topic)
				return
			}
			select {
			case s.Ch <- msg:
				// Message successfully delivered to public channel
			case <-s.close:
				logDebug("Subscriber %s (topic '%s') delivery: s.close signal received while sending to s.Ch. Message dropped: %v", s.ID, s.Topic, msg.Data)
				cleanupMessageData(msg.Data)
			}
		case <-s.close:
			logDebug("Subscriber %s (topic '%s') delivery: s.close signal received. Initiating internalCh closure.", s.ID, s.Topic)
			s.internalCloseOnce.Do(func() {
				close(s.internalCh)
			})
			// After closing internalCh, drain any remaining buffered messages.
			for msg := range s.internalCh {
				logDebug("Subscriber %s (topic '%s') delivery: Draining message from internalCh on shutdown: %v", s.ID, s.Topic, msg.Data)
				cleanupMessageData(msg.Data)
			}
		}
	}
}

// CleanupSub performs the actual unsubscription and cleanup of a subscriber. It is intended
// for internal use and is called by Subscriber.Unsubscribe() or PubSub.Close().
func (ps *PubSub) CleanupSub(sub *Subscriber) {
	if sub == nil {
		logDebug("CleanupSub called with nil subscriber.")
		return
	}

	originalSubID := sub.ID
	originalSubTopic := sub.Topic
	instanceActuallyRemovedFromMap := false

	func() {
		ps.mu.Lock()
		defer ps.mu.Unlock()
		topicSubscribers, topicExists := ps.subscribers[originalSubTopic]
		if !topicExists {
			return
		}
		existingSubInMap, subExistsInMap := topicSubscribers[originalSubID]
		if !subExistsInMap {
			return
		}
		if existingSubInMap == sub {
			delete(topicSubscribers, originalSubID)
			if len(topicSubscribers) == 0 {
				delete(ps.subscribers, originalSubTopic)
			}
			instanceActuallyRemovedFromMap = true
		}
	}()

	logDebug("CleanupSub: Initiating shutdown signal for subscriber '%s'. Instance removed: %t", sub.ID, instanceActuallyRemovedFromMap)
	sub.shutdownOnce.Do(func() {
		close(sub.close)
		logDebug("CleanupSub: Signaled close for subscriber '%s'.", sub.ID)
	})

	go func(s *Subscriber) {
		logDebug("Subscriber '%s' - starting drainer for public channel s.Ch.", s.ID)
		for msg := range s.Ch {
			cleanupMessageData(msg.Data) // Cleanup any messages drained from public channel
		}
		logDebug("Subscriber '%s' - finished draining public channel s.Ch.", s.ID)
	}(sub)

	// Wait for the deliverMessages goroutine to finish its shutdown sequence.
	sub.deliveryWg.Wait()

	// After the deliverMessages goroutine has stopped, we can safely drain any
	// remaining messages from internalCh. This handles the race condition where
	// deliverMessages might not have had time to drain everything.
	sub.internalCloseOnce.Do(func() {
		close(sub.internalCh)
	})
	for msg := range sub.internalCh {
		logDebug("CleanupSub: Draining message from internalCh: %v", msg.Data)
		cleanupMessageData(msg.Data)
	}

	logDebug("Subscriber '%s' cleanup complete.", originalSubID)
}

// SendReceive subscribes, publishes, and waits for a response. It returns the data from the
// received message or nil if timed out or an error occurred.
func (ps *PubSub) SendReceive(sendTopic string, receiveTopic string, sendMsg interface{}, timeoutMs int) (result interface{}) {
	subID := ps.GetUniqueSubscriberID()
	sub := ps.Subscribe(receiveTopic, subID, 1)
	if sub == nil {
		logDebug("SendReceive: Failed to subscribe to receive topic '%s'.", receiveTopic)
		return nil
	}
	defer sub.Unsubscribe()

	ps.Publish(Message{Topic: sendTopic, Data: sendMsg})

	select {
	case msg, ok := <-sub.Ch:
		if ok {
			result = msg.Data
		} else {
			logDebug("SendReceive: Subscriber channel closed before message received.", receiveTopic)
		}
	case <-time.After(time.Millisecond * time.Duration(timeoutMs)):
		logDebug("SendReceive timeout of %dms reached for receive topic '%s'.", timeoutMs, receiveTopic)
	}
	return
}

// cleanupMessageData checks if data implements Cleanable and calls Cleanup if so.
func cleanupMessageData(data interface{}) {
	if cleanable, ok := data.(Cleanable); ok {
		logDebug("Message data implements Cleanable, calling Cleanup().")
		cleanable.Cleanup()
	}
}

// Publish sends a message to all relevant subscribers of the message's topic.
// Delivery behavior (dropping vs. blocking) depends on the topic's configuration.
func (ps *PubSub) Publish(message Message) {
	topicConfig := ps.getTopicConfig(message.Topic)

	ps.mu.RLock()
	var subsToNotify []*Subscriber
	if topicSubs, ok := ps.subscribers[message.Topic]; ok {
		subsToNotify = make([]*Subscriber, 0, len(topicSubs))
		for _, sub := range topicSubs {
			subsToNotify = append(subsToNotify, sub)
		}
	}
	ps.mu.RUnlock()

	if len(subsToNotify) == 0 {
		logDebug("No subscribers for topic '%s'. Cleaning up message.", message.Topic)
		cleanupMessageData(message.Data) // No subscribers, message is effectively dropped.
		return
	}

	logDebug("Publishing message to topic '%s' to %d subscribers.", message.Topic, len(subsToNotify))

	var wg sync.WaitGroup
	for _, currentSub := range subsToNotify {
		wg.Add(1)
		go func(sub *Subscriber, m Message, tc TopicConfig) {
			defer wg.Done()

			func() {
				defer func() {
					if r := recover(); r != nil {
						logDebug("PANIC recovered in Publish send logic for sub %s: %v. Cleaning up message.", sub.ID, r)
						cleanupMessageData(m.Data)
					}
				}()

				select {
				case <-sub.close:
					logDebug("Warning: Not sending to subscriber '%s' as it's closing. Cleaning up message.", sub.ID)
					cleanupMessageData(m.Data)
					return
				default:
				}

				if tc.AllowDropping {
					select {
					case sub.internalCh <- m:
					default:
						logDebug("Warning: Dropping message for subscriber '%s' (channel full, dropping allowed). Cleaning up message.", sub.ID)
						cleanupMessageData(m.Data)
					}
				} else { // Blocking send
					var C_timeoutChan <-chan time.Time
					var timer *time.Timer
					if tc.PublishTimeout > 0 {
						timer = time.NewTimer(tc.PublishTimeout)
						C_timeoutChan = timer.C
						defer timer.Stop()
					}
					select {
					case <-sub.close:
						logDebug("Publish/Block: Subscriber '%s' closed. Cleaning up message.", sub.ID)
						cleanupMessageData(m.Data)
					case <-C_timeoutChan:
						logDebug("Publish/Block: Timeout for subscriber '%s'. Cleaning up message.", sub.ID)
						cleanupMessageData(m.Data)
					case sub.internalCh <- m:
					}
				}
			}()
		}(currentSub, message, topicConfig)
	}
	wg.Wait()
}

// Close gracefully shuts down the PubSub system. It unsubscribes all active subscribers
// and ensures their resources are released.
func (ps *PubSub) Close() {
	logDebug("Initiating graceful shutdown of PubSub system.")

	allSubscribers := func() []*Subscriber {
		ps.mu.Lock()
		defer ps.mu.Unlock()

		subs := make([]*Subscriber, 0)
		if ps.subscribers != nil {
			for _, topicSubs := range ps.subscribers {
				for _, sub := range topicSubs {
					subs = append(subs, sub)
				}
			}
			ps.subscribers = make(map[string]map[string]*Subscriber)
			ps.topicConfigs = make(map[string]TopicConfig)
			logDebug("PubSub subscribers and topicConfigs maps cleared.")
		}
		return subs
	}()

	var wg sync.WaitGroup
	logDebug("PubSub.Close: Cleaning up %d subscribers.", len(allSubscribers))
	for _, subInstance := range allSubscribers {
		wg.Add(1)
		go func(s *Subscriber) {
			defer wg.Done()
			logDebug("PubSub.Close: Triggering Unsubscribe for subscriber '%s'.", s.ID)
			s.Unsubscribe()
			logDebug("PubSub.Close: Unsubscribe call completed for subscriber '%s'.", s.ID)
		}(subInstance)
	}
	wg.Wait()
	logDebug("PubSub system closed gracefully.")
}

// ReadMessages is a helper function for clients to continuously read messages
// from a subscriber's channel using a provided handler function. This function will
// block until the subscriber's channel is closed.
func (s *Subscriber) ReadMessages(handler func(Message)) {
	logDebug("Subscriber %s (topic '%s') ReadMessages loop started.", s.ID, s.Topic)
	for msg := range s.Ch {
		logDebug("Subscriber %s (topic '%s') ReadMessages calling handler.", s.ID, s.Topic)
		handler(msg)
	}
	logDebug("Subscriber %s (topic '%s') ReadMessages loop exited (s.Ch closed).", s.ID, s.Topic)
}
