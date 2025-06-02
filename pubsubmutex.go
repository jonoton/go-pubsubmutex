package pubsubmutex

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
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

// Message represents a message to be published.
type Message struct {
	Topic string
	Data  interface{}
}

// TopicConfig allows configuring behavior for a specific topic.
type TopicConfig struct {
	// AllowDropping, if true, means messages published to this topic
	// might be dropped if a subscriber's internal buffer is full.
	// If false (default), publishing will block until the message is accepted
	// by the subscriber's internal buffer, guaranteeing no drops.
	AllowDropping bool
}

// Subscriber represents a subscriber to a topic.
type Subscriber struct {
	ID         string         // Unique identifier for the subscriber
	Ch         chan Message   // Public channel for the subscriber to receive messages
	internalCh chan Message   // Internal buffer for messages, filled by publisher
	close      chan struct{}  // Channel to signal closure to the delivery goroutine
	deliveryWg sync.WaitGroup // To wait for the dedicated delivery goroutine to finish
}

// PubSub represents the pub/sub system.
type PubSub struct {
	mu           sync.RWMutex
	subscribers  map[string]map[string]*Subscriber // topic -> subscriberID -> *Subscriber
	topicConfigs map[string]TopicConfig            // topic -> TopicConfig
}

// NewPubSub creates a new PubSub system.
func NewPubSub() *PubSub {
	logDebug("New PubSub system created.")
	return &PubSub{
		subscribers:  make(map[string]map[string]*Subscriber),
		topicConfigs: make(map[string]TopicConfig),
	}
}

// CreateTopic explicitly creates and configures a topic.
// This allows setting topic-specific behaviors like message dropping.
// If a topic is not explicitly created, it defaults to no-dropping behavior.
func (ps *PubSub) CreateTopic(topic string, config TopicConfig) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.topicConfigs[topic] = config
	logDebug("Topic '%s' created with config: %+v", topic, config)
}

// getTopicConfig retrieves the configuration for a given topic.
// If no specific config is set, it returns a default (no-dropping) config.
func (ps *PubSub) getTopicConfig(topic string) TopicConfig {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	if config, ok := ps.topicConfigs[topic]; ok {
		return config
	}
	// Default behavior: no dropping
	return TopicConfig{AllowDropping: false}
}

// Subscribe allows a subscriber to register for a specific topic.
// It returns a Subscriber struct which contains a channel to receive messages.
//
// bufferSize determines the capacity of the internal message channel for this subscriber.
func (ps *PubSub) Subscribe(topic string, subscriberID string, bufferSize int) *Subscriber {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if _, ok := ps.subscribers[topic]; !ok {
		ps.subscribers[topic] = make(map[string]*Subscriber)
		logDebug("Created new topic map for '%s'.", topic)
	}

	sub := &Subscriber{
		ID:         subscriberID,
		Ch:         make(chan Message, bufferSize), // Public channel for the consumer
		internalCh: make(chan Message, bufferSize), // Internal buffer for publishers
		close:      make(chan struct{}),
	}
	ps.subscribers[topic][subscriberID] = sub

	// Start a dedicated goroutine for this subscriber to deliver messages
	sub.deliveryWg.Add(1)
	go sub.deliverMessages()

	logDebug("Subscriber '%s' subscribed to topic '%s' with buffer size %d.", subscriberID, topic, bufferSize)
	return sub
}

// GetUniqueSubscriberID is a helper function that generates a unique Subscriber ID.
func (p *PubSub) GetUniqueSubscriberID() string {
	return uuid.New().String()
}

// SendReceive allows a message to be sent on a topic and receive a response on another topic with a timeout.
func (p *PubSub) SendReceive(sendTopic string, receiveTopic string, sendMsg interface{}, timeoutMs int) (result interface{}) {
	sub := p.Subscribe(receiveTopic, p.GetUniqueSubscriberID(), 1)
	defer p.Unsubscribe(receiveTopic, sub.ID)
	p.Publish(Message{Topic: sendTopic, Data: sendMsg})
	select {
	case msg, ok := <-sub.Ch:
		if ok {
			result = msg.Data
		}
	case <-time.After(time.Millisecond * time.Duration(timeoutMs)):
		break
	}
	return
}

// deliverMessages is a goroutine run for each subscriber to pull messages
// from its internal buffer and push them to its public channel.
// This decouples the publisher from the subscriber's consumption rate.
func (s *Subscriber) deliverMessages() {
	defer s.deliveryWg.Done() // Signal completion when this goroutine exits
	defer close(s.Ch)         // Close the public channel when delivery stops

	logDebug("Subscriber %s delivery goroutine started.", s.ID)
	for {
		select {
		case msg, ok := <-s.internalCh:
			if !ok {
				// internalCh has been closed, no more messages will arrive.
				// Drain any remaining messages in internalCh before exiting.
				for msg := range s.internalCh {
					logDebug("Subscriber %s draining remaining message for topic '%s' to public channel.", s.ID, msg.Topic)
					s.Ch <- msg // Deliver remaining messages
				}
				logDebug("Subscriber %s delivery goroutine exiting (internal channel closed).", s.ID)
				return
			}
			// This send will block if s.Ch is full, ensuring message delivery.
			logDebug("Subscriber %s sending message for topic '%s' to public channel.", s.ID, msg.Topic)
			s.Ch <- msg
		case <-s.close:
			// Received signal to close. Close internalCh to allow draining
			// and then exit the loop.
			logDebug("Subscriber %s delivery goroutine received close signal.", s.ID)
			close(s.internalCh) // Signal internalCh to close, which will trigger the !ok case above
			return
		}
	}
}

// Unsubscribe removes a subscriber from a topic.
// It gracefully stops the subscriber's delivery goroutine and closes channels.
func (ps *PubSub) Unsubscribe(topic string, subscriberID string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if subs, ok := ps.subscribers[topic]; ok {
		if sub, found := subs[subscriberID]; found {
			logDebug("Attempting to unsubscribe subscriber '%s' from topic '%s'.", subscriberID, topic)
			// 1. Signal the delivery goroutine to stop first.
			close(sub.close)

			// 2. Wait for the delivery goroutine to finish.
			sub.deliveryWg.Wait()

			// 3. Clean up the subscriber from the map.
			delete(subs, subscriberID)
			if len(subs) == 0 {
				delete(ps.subscribers, topic)
				logDebug("Topic '%s' has no more subscribers, removing topic entry.", topic)
			}
			logDebug("Subscriber '%s' unsubscribed successfully from topic '%s'.", subscriberID, topic)
		} else {
			logDebug("Subscriber '%s' not found for topic '%s'.", subscriberID, topic)
		}
	} else {
		logDebug("Topic '%s' not found for unsubscription.", topic)
	}
}

// Publish sends a message to all subscribers of a specific topic.
// Behavior (dropping vs. blocking) depends on the topic's configuration.
func (ps *PubSub) Publish(message Message) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	logDebug("Publishing message to topic '%s': %v", message.Topic, message.Data)

	topicConfig := ps.getTopicConfig(message.Topic)

	if subs, ok := ps.subscribers[message.Topic]; ok {
		var wg sync.WaitGroup // To wait for all sends to internal channels
		for _, sub := range subs {
			wg.Add(1)
			go func(s *Subscriber) {
				defer wg.Done()
				select {
				case <-s.close:
					// Subscriber is in the process of closing, don't send message
					logDebug("Warning: Not sending message to subscriber '%s' (topic '%s') as it's closing.", s.ID, message.Topic)
				case s.internalCh <- message:
					// Message successfully sent to internal channel (blocking or non-blocking, depending on context)
					logDebug("Message delivered to internal channel for subscriber '%s' on topic '%s'.", s.ID, message.Topic)
				default:
					if topicConfig.AllowDropping {
						// Only hit this default if AllowDropping is true AND the channel is full
						logDebug("Warning: Dropping message for subscriber '%s' on topic '%s' (channel full, dropping allowed).", s.ID, message.Topic)
					} else {
						// This branch should ideally not be hit if AllowDropping is false,
						// as the blocking send case `s.internalCh <- message` should take precedence.
						// It's a fallback for unexpected scenarios.
						logDebug("Error: Unexpected message drop for subscriber '%s' on topic '%s' (dropping not allowed, channel full). This indicates a logic error or extreme saturation.", s.ID, message.Topic)
					}
				}
			}(sub)
		}
		wg.Wait() // Wait for all messages to be processed by send goroutines
		logDebug("All internal sends for topic '%s' completed (or dropped).", message.Topic)
	} else {
		logDebug("No subscribers found for topic '%s'. Message not delivered to anyone.", message.Topic)
	}
}

// Close closes all subscriber channels and cleans up the PubSub system.
// It ensures all delivery goroutines are stopped gracefully.
func (ps *PubSub) Close() {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	logDebug("Initiating graceful shutdown of PubSub system.")
	for topic, subs := range ps.subscribers {
		for subscriberID, sub := range subs {
			logDebug("Closing subscriber '%s' for topic '%s'.", subscriberID, topic)
			// Signal and wait for each subscriber's delivery goroutine
			close(sub.close)
			sub.deliveryWg.Wait() // Wait for delivery goroutine to finish

			// Clean up map entries (this happens after the loop for topics is done)
			delete(subs, subscriberID)
		}
		delete(ps.subscribers, topic)
	}
	// Clear topic configurations as well
	ps.topicConfigs = make(map[string]TopicConfig)
	logDebug("PubSub system closed gracefully.")
}

// ReadMessages is a helper function for subscribers to read messages.
// It runs in a goroutine and continuously reads from the subscriber's channel
// until the channel is closed.
func (s *Subscriber) ReadMessages(handler func(Message)) {
	logDebug("Subscriber %s consumer goroutine started.", s.ID)
	for msg := range s.Ch { // This loop will exit when s.Ch is closed
		logDebug("Subscriber %s calling handler for message on topic '%s'.", s.ID, msg.Topic)
		handler(msg)
	}
	logDebug("Subscriber %s consumer goroutine exiting.", s.ID)
}
