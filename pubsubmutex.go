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
func logDebug(format string, a ...interface{}) {
	if debugEnabled {
		fmt.Printf("[PUBSUB DEBUG] "+format+"\n", a...)
	}
}

// uniqueIDCounter is used to generate unique subscriber IDs.
var uniqueIDCounter atomic.Uint64

// GetUniqueSubscriberID generates a unique subscriber ID.
func (ps *PubSub) GetUniqueSubscriberID() string {
	return fmt.Sprintf("sub-%d", uniqueIDCounter.Add(1))
}

// Message represents a message to be published.
type Message struct {
	Topic string
	Data  interface{}
}

// DefaultPublishTimeout is the default duration a publisher will wait
// for a message to be accepted if no specific TopicConfig is provided
// and AllowDropping is false.
const DefaultPublishTimeout = 500 * time.Millisecond

// TopicConfig allows configuring behavior for a specific topic.
type TopicConfig struct {
	AllowDropping  bool
	PublishTimeout time.Duration
}

// Subscriber represents a subscriber to a topic.
type Subscriber struct {
	ID                string
	Topic             string
	Ch                chan Message
	internalCh        chan Message
	close             chan struct{}
	shutdownOnce      sync.Once // For closing s.close channel once
	deliveryWg        sync.WaitGroup
	internalCloseOnce sync.Once // For closing s.internalCh channel once

	// New fields for self-unsubscription
	unsubscribeFunc func()      // Function to call to initiate cleanup via PubSub
	unsubscribed    atomic.Bool // To ensure Unsubscribe() logic runs once
}

// PubSub represents the pub/sub system.
type PubSub struct {
	mu           sync.RWMutex
	subscribers  map[string]map[string]*Subscriber // topic -> subscriberID -> *Subscriber
	topicConfigs map[string]TopicConfig            // topic -> TopicConfig
}

// NewPubSub creates a new PubSub system.
func NewPubSub() *PubSub {
	ps := &PubSub{
		subscribers:  make(map[string]map[string]*Subscriber),
		topicConfigs: make(map[string]TopicConfig),
	}
	logDebug("New PubSub system created.")
	return ps
}

// CreateTopic explicitly creates and configures a topic.
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

// Subscribe allows a subscriber to register for a specific topic.
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
		// This log is within the lock, but it's an error path and should be rare.
		logDebug("Error: Subscriber '%s' already exists for topic '%s'. Returning nil.", subscriberID, topic)
		return nil
	}

	sub := &Subscriber{
		ID:         subscriberID,
		Topic:      topic,
		Ch:         make(chan Message),
		internalCh: make(chan Message, bufferSize),
		close:      make(chan struct{}),
		// unsubscribed is initialized to false (zero value for atomic.Bool)
		// shutdownOnce and internalCloseOnce are initialized to their zero values
	}

	// Create and assign the unsubscribe callback
	sub.unsubscribeFunc = func() {
		logDebug("Subscriber %s (topic '%s') initiated self-unsubscription via unsubscribeFunc.", sub.ID, sub.Topic)
		ps.CleanupSub(sub) // Call the existing central cleanup logic
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

// Unsubscribe signals the PubSub system to remove and clean up this subscriber.
// It's safe to call multiple times; the actual unsubscription process will only occur once.
func (s *Subscriber) Unsubscribe() {
	// Atomically check and set the unsubscribed flag.
	// CompareAndSwap(old, new): if current value is 'false', set to 'true' and return 'true'.
	if s.unsubscribed.CompareAndSwap(false, true) {
		if s.unsubscribeFunc != nil {
			logDebug("Subscriber %s (topic '%s'): Calling unsubscribeFunc.", s.ID, s.Topic)
			s.unsubscribeFunc()
		} else {
			logDebug("Subscriber %s (topic '%s'): Unsubscribe called but unsubscribeFunc is nil (already unsubscribed or detached).", s.ID, s.Topic)
		}
	} else {
		logDebug("Subscriber %s (topic '%s'): Unsubscribe called but already in process or completed.", s.ID, s.Topic)
	}
}

// deliverMessages is a goroutine run for each subscriber.
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
			if !ok { // internalCh was closed (by logic in 'case <-s.close' path below).
				logDebug("Subscriber %s (topic '%s') delivery: internalCh closed. Exiting.", s.ID, s.Topic)
				return
			}

			logDebug("Subscriber %s (topic '%s') delivery: attempting to send to s.Ch: %v", s.ID, s.Topic, msg.Data)
			select {
			case s.Ch <- msg:
				logDebug("Subscriber %s (topic '%s') delivery: sent to s.Ch: %v", s.ID, s.Topic, msg.Data)
			case <-s.close: // If s.close is closed, this path is non-blocking.
				logDebug("Subscriber %s (topic '%s') delivery: s.close signal received while sending to s.Ch. Message dropped: %v", s.ID, s.Topic, msg.Data)
				// Do not call s.internalCloseOnce.Do here.
				// The main `case <-s.close:` path is solely responsible for initiating internalCh closure.
			}

		case <-s.close: // External signal to shut down (from CleanupSub or PubSub.Close)
			logDebug("Subscriber %s (topic '%s') delivery: s.close signal received. Initiating internalCh closure.", s.ID, s.Topic)
			s.internalCloseOnce.Do(func() {
				close(s.internalCh)
			})
			// The loop continues. `case msg, ok := <-s.internalCh` will detect the closure of internalCh
			// (after draining buffered items) and then exit via the `if !ok { return }` path.
		}
	}
}

// CleanupSub unsubscribes a subscriber. Called by PubSub system or via Subscriber.Unsubscribe.
func (ps *PubSub) CleanupSub(sub *Subscriber) {
	if sub == nil {
		logDebug("CleanupSub called with nil subscriber.")
		return
	}

	var topic string
	var subID string
	var validInstanceForCleanup bool

	func() { // Scoped lock for map manipulation.
		ps.mu.Lock()
		defer ps.mu.Unlock()

		topic = sub.Topic
		subID = sub.ID

		topicSubscribers, topicExists := ps.subscribers[topic]
		if !topicExists {
			logDebug("CleanupSub: Topic '%s' not found for subscriber '%s'.", topic, subID)
			return
		}

		existingSub, subExists := topicSubscribers[subID]
		if !subExists {
			logDebug("CleanupSub: Subscriber ID '%s' not found in topic '%s'.", subID, topic)
			return
		}
		if existingSub != sub {
			logDebug("CleanupSub: Subscriber instance mismatch for ID '%s' in topic '%s'. Given %p, found %p. Not removing from map via this call.", subID, topic, sub, existingSub)
			// If it's not the same instance, another CleanupSub might be responsible for the one in the map.
			// However, the passed 'sub' instance still needs its own resources (like 'close' channel) handled.
			return
		}

		logDebug("CleanupSub: Removing subscriber '%s' from topic '%s'.", subID, topic)
		delete(topicSubscribers, subID)
		if len(topicSubscribers) == 0 {
			logDebug("CleanupSub: Topic '%s' has no more subscribers, removing topic entry.", topic)
			delete(ps.subscribers, topic)
		}
		validInstanceForCleanup = true // map removal was successful for this instance
	}()

	if !validInstanceForCleanup {
		// Log that map removal was skipped, but we'll still proceed with signaling this specific 'sub' instance.
		logDebug("CleanupSub: Subscriber '%s' (topic '%s') map removal skipped or instance mismatch. Proceeding with shutdown signal.", sub.ID, sub.Topic)
	}

	logDebug("CleanupSub: Initiating shutdown signal for subscriber '%s' (topic '%s').", sub.ID, sub.Topic)
	sub.shutdownOnce.Do(func() {
		close(sub.close)
		logDebug("CleanupSub: Signaled close (via shutdownOnce) for subscriber '%s'.", sub.ID)
	})

	// Goroutine to drain the public channel.
	// This helps deliverMessages unblock if it's trying to send to sub.Ch.
	go func(s *Subscriber) {
		logDebug("Subscriber '%s' (topic '%s') - CleanupSub: starting drainer for public channel s.Ch.", s.ID, s.Topic)
		for range s.Ch { /* Drain messages */
		}
		logDebug("Subscriber '%s' (topic '%s') - CleanupSub: finished draining public channel s.Ch.", s.ID, s.Topic)
	}(sub)

	sub.deliveryWg.Wait() // Wait for deliverMessages to complete.
	logDebug("Subscriber '%s' (topic '%s') cleanup complete.", sub.ID, sub.Topic)
}

// SendReceive (remains same)
func (ps *PubSub) SendReceive(sendTopic string, receiveTopic string, sendMsg interface{}, timeoutMs int) (result interface{}) {
	subID := ps.GetUniqueSubscriberID()
	sub := ps.Subscribe(receiveTopic, subID, 1)
	if sub == nil {
		logDebug("SendReceive: Failed to subscribe to receive topic '%s'.", receiveTopic)
		return nil
	}
	defer sub.Unsubscribe() // Use the new subscriber method

	ps.Publish(Message{Topic: sendTopic, Data: sendMsg})

	select {
	case msg, ok := <-sub.Ch:
		if ok {
			result = msg.Data
		} else {
			logDebug("SendReceive: Subscriber channel for topic '%s' closed before message received.", receiveTopic)
		}
	case <-time.After(time.Millisecond * time.Duration(timeoutMs)):
		logDebug("SendReceive timeout of %dms reached for receive topic '%s'.", timeoutMs, receiveTopic)
	}
	return
}

// Publish (remains same - with recover)
func (ps *PubSub) Publish(message Message) {
	ps.mu.RLock()
	topicConfig := ps.getTopicConfig(message.Topic)
	var subsToNotify []*Subscriber
	if topicSubs, ok := ps.subscribers[message.Topic]; ok {
		subsToNotify = make([]*Subscriber, 0, len(topicSubs))
		for _, sub := range topicSubs {
			subsToNotify = append(subsToNotify, sub)
		}
	}
	ps.mu.RUnlock()

	if len(subsToNotify) == 0 {
		return
	}

	logDebug("Publishing message to topic '%s' to %d subscribers: %v", message.Topic, len(subsToNotify), message.Data)

	var wg sync.WaitGroup
	for _, currentSub := range subsToNotify {
		wg.Add(1)
		go func(s *Subscriber, m Message, tc TopicConfig) {
			defer wg.Done()

			func() {
				defer func() {
					if r := recover(); r != nil {
						logDebug("PANIC recovered in Publish send logic for sub %s (topic %s): %v. Message data: %v. Dropped.", s.ID, s.Topic, r, m.Data)
					}
				}()

				select {
				case <-s.close:
					logDebug("Warning: Not sending to subscriber '%s' (topic '%s') as it's closing (initial check).", s.ID, s.Topic)
					return
				default:
				}

				if tc.AllowDropping {
					select {
					case <-s.close:
						logDebug("Publish/Drop: Subscriber '%s' closed before non-blocking send attempt.", s.ID)
					default:
						select {
						case s.internalCh <- m:
							logDebug("Message delivered (non-blocking) to internal channel for subscriber '%s'.", s.ID)
						default:
							logDebug("Warning: Dropping message for subscriber '%s' (channel full or closing, dropping allowed).", s.ID)
						}
					}
				} else {
					var C_timeoutChan <-chan time.Time
					var timer *time.Timer
					if tc.PublishTimeout > 0 {
						timer = time.NewTimer(tc.PublishTimeout)
						C_timeoutChan = timer.C
						defer timer.Stop()
					}
					select {
					case <-s.close:
						logDebug("Publish/Block: Subscriber '%s' closed. Not sending.", s.ID, s.Topic)
					case <-C_timeoutChan:
						logDebug("Publish/Block: Timeout (%s) for subscriber '%s' on topic '%s'. Message not delivered.", tc.PublishTimeout, s.ID, s.Topic)
					case s.internalCh <- m:
						logDebug("Message delivered (blocking) to internal channel for subscriber '%s'.", s.ID)
					}
				}
			}()
		}(currentSub, message, topicConfig)
	}
	wg.Wait()
	logDebug("All internal sends for topic '%s' completed (or dropped/timed out/panicked internally).", message.Topic)
}

// Close (remains same - with shutdownOnce)
func (ps *PubSub) Close() {
	logDebug("Initiating graceful shutdown of PubSub system.")

	allSubscribers := func() []*Subscriber {
		ps.mu.Lock()
		defer ps.mu.Unlock()

		subs := make([]*Subscriber, 0)
		for _, topicSubs := range ps.subscribers {
			for _, sub := range topicSubs {
				subs = append(subs, sub)
			}
		}
		ps.subscribers = make(map[string]map[string]*Subscriber)
		ps.topicConfigs = make(map[string]TopicConfig)
		return subs
	}()

	var wg sync.WaitGroup
	for _, subInstance := range allSubscribers {
		wg.Add(1)
		go func(s *Subscriber) {
			defer wg.Done()
			logDebug("Closing subscriber '%s' for topic '%s' during PubSub Close.", s.ID, s.Topic)
			s.shutdownOnce.Do(func() { // Use shutdownOnce to safely close s.close
				close(s.close)
				logDebug("PubSub.Close: Signaled close (via shutdownOnce) for subscriber '%s'.", s.ID)
			})

			go func(sDrain *Subscriber) {
				logDebug("Subscriber '%s' (topic '%s') - PubSub.Close: starting drain for public channel s.Ch.", sDrain.ID, sDrain.Topic)
				for range sDrain.Ch { /* Drain */
				}
				logDebug("Subscriber '%s' (topic '%s') - PubSub.Close: finished draining public channel s.Ch.", sDrain.ID, sDrain.Topic)
			}(s)

			s.deliveryWg.Wait()
			logDebug("Subscriber '%s' (topic '%s') fully shut down during PubSub Close.", s.ID, s.Topic)
		}(subInstance)
	}
	wg.Wait()
	logDebug("PubSub system closed gracefully.")
}

// ReadMessages (remains same)
func (s *Subscriber) ReadMessages(handler func(Message)) {
	logDebug("Subscriber %s (topic '%s') consumer ReadMessages goroutine started.", s.ID, s.Topic)
	for msg := range s.Ch {
		logDebug("Subscriber %s (topic '%s') ReadMessages calling handler for: %v", s.ID, s.Topic, msg.Data)
		handler(msg)
	}
	logDebug("Subscriber %s (topic '%s') consumer ReadMessages goroutine exiting (s.Ch closed).", s.ID, s.Topic)
}
