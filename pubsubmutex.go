package pubsubmutex

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ManagedItem is an optional generic interface for message data that requires
// reference counting and explicit cleanup. The generic type T ensures that
// Ref() returns the concrete type of the data.
type ManagedItem[T any] interface {
	Ref() T
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

// Message represents a message to be published. It is generic over the type
// of its data payload.
type Message[T any] struct {
	Topic string
	Data  T
}

// DefaultPublishTimeout is the default duration a publisher will wait.
const DefaultPublishTimeout = 500 * time.Millisecond

// TopicConfig allows configuring behavior for a specific topic.
type TopicConfig struct {
	AllowDropping  bool
	PublishTimeout time.Duration
}

// Subscriber represents a client subscribed to a topic. It is generic over
// the type of data it receives.
type Subscriber[T any] struct {
	ID                string
	Topic             string
	Ch                chan Message[T]
	internalCh        chan Message[T]
	close             chan struct{}
	shutdownOnce      sync.Once
	deliveryWg        sync.WaitGroup
	internalCloseOnce sync.Once
	unsubscribeFunc   func()
	unsubscribed      atomic.Bool
}

// Unsubscribe signals the PubSub system to remove and clean up this subscriber instance.
func (s *Subscriber[T]) Unsubscribe() {
	if s.unsubscribed.CompareAndSwap(false, true) {
		if s.unsubscribeFunc != nil {
			s.unsubscribeFunc()
		}
	}
}

// ReadMessages is a helper function for clients to continuously read messages.
func (s *Subscriber[T]) ReadMessages(handler func(Message[T])) {
	logDebug("Subscriber %s: ReadMessages loop started.", s.ID)
	for msg := range s.Ch {
		handler(msg)
	}
	logDebug("Subscriber %s: ReadMessages loop exited.", s.ID)
}

// pubSubCore is an interface for the internal, non-generic methods of a basePubSub.
type pubSubCore interface {
	updateTopic(topic string, config TopicConfig)
	close()
}

// basePubSub represents the internal, core publish-subscribe system. It is generic
// over the type of data it handles and is not intended for direct use.
type basePubSub[T any] struct {
	mu           sync.RWMutex
	subscribers  map[string]map[string]*Subscriber[T]
	topicConfigs map[string]TopicConfig
}

// newBasePubSub creates a new internal generic PubSub system instance.
func newBasePubSub[T any]() *basePubSub[T] {
	ps := &basePubSub[T]{
		subscribers:  make(map[string]map[string]*Subscriber[T]),
		topicConfigs: make(map[string]TopicConfig),
	}
	logDebug("New basePubSub system created for type %T.", *new(T))
	return ps
}

func (ps *basePubSub[T]) updateTopic(topic string, config TopicConfig) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.topicConfigs[topic] = config
}

func (ps *basePubSub[T]) getTopicConfig(topic string) TopicConfig {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	if config, ok := ps.topicConfigs[topic]; ok {
		return config
	}
	return TopicConfig{AllowDropping: false, PublishTimeout: DefaultPublishTimeout}
}

func (ps *basePubSub[T]) subscribe(topic string, subscriberID string, bufferSize int) *Subscriber[T] {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if bufferSize < 0 {
		bufferSize = 0
	}

	if _, ok := ps.subscribers[topic]; !ok {
		ps.subscribers[topic] = make(map[string]*Subscriber[T])
	}

	if _, exists := ps.subscribers[topic][subscriberID]; exists {
		return nil
	}

	sub := &Subscriber[T]{
		ID:         subscriberID,
		Topic:      topic,
		Ch:         make(chan Message[T]),
		internalCh: make(chan Message[T], bufferSize),
		close:      make(chan struct{}),
	}

	sub.unsubscribeFunc = func() {
		ps.cleanupSub(sub)
	}

	ps.subscribers[topic][subscriberID] = sub
	sub.deliveryWg.Add(1)
	go sub.deliverMessages()

	return sub
}

func (s *Subscriber[T]) deliverMessages() {
	defer s.deliveryWg.Done()
	defer close(s.Ch)

	for {
		select {
		case msg, ok := <-s.internalCh:
			if !ok {
				return
			}
			select {
			case s.Ch <- msg:
			case <-s.close:
				cleanupOrDrop(msg.Data)
			}
		case <-s.close:
			s.internalCloseOnce.Do(func() { close(s.internalCh) })
			for msg := range s.internalCh {
				cleanupOrDrop(msg.Data)
			}
		}
	}
}

func (ps *basePubSub[T]) cleanupSub(sub *Subscriber[T]) {
	if sub == nil {
		return
	}
	func() {
		ps.mu.Lock()
		defer ps.mu.Unlock()
		if topicSubs, ok := ps.subscribers[sub.Topic]; ok {
			if existingSub, found := topicSubs[sub.ID]; found && existingSub == sub {
				delete(topicSubs, sub.ID)
				if len(topicSubs) == 0 {
					delete(ps.subscribers, sub.Topic)
				}
			}
		}
	}()
	sub.shutdownOnce.Do(func() { close(sub.close) })
	sub.deliveryWg.Wait()
	go func() {
		for msg := range sub.Ch {
			cleanupOrDrop(msg.Data)
		}
	}()
}

func (ps *basePubSub[T]) publish(message Message[T]) {
	topicConfig := ps.getTopicConfig(message.Topic)
	ps.mu.RLock()
	var subsToNotify []*Subscriber[T]
	if topicSubs, ok := ps.subscribers[message.Topic]; ok {
		subsToNotify = make([]*Subscriber[T], 0, len(topicSubs))
		for _, sub := range topicSubs {
			subsToNotify = append(subsToNotify, sub)
		}
	}
	ps.mu.RUnlock()

	numSubs := len(subsToNotify)
	if numSubs == 0 {
		cleanupOrDrop(message.Data)
		return
	}

	refs := make([]T, numSubs)
	refs[0] = message.Data
	if managed, ok := any(message.Data).(ManagedItem[T]); ok {
		for i := 1; i < numSubs; i++ {
			refs[i] = managed.Ref()
		}
	} else {
		for i := 1; i < numSubs; i++ {
			refs[i] = message.Data
		}
	}

	var wg sync.WaitGroup
	for i, currentSub := range subsToNotify {
		wg.Add(1)
		go func(sub *Subscriber[T], dataRef T, msgTopic string, tc TopicConfig) {
			defer wg.Done()
			sent := false
			defer func() {
				if r := recover(); r != nil {
					logDebug("PANIC recovered in Publish send logic: %v", r)
				}
				if !sent {
					cleanupOrDrop(dataRef)
				}
			}()

			select {
			case <-sub.close:
				return
			default:
			}
			msgToSend := Message[T]{Topic: msgTopic, Data: dataRef}
			if tc.AllowDropping {
				select {
				case sub.internalCh <- msgToSend:
					sent = true
				default:
				}
			} else {
				var timeoutChan <-chan time.Time
				if tc.PublishTimeout > 0 {
					timer := time.NewTimer(tc.PublishTimeout)
					defer timer.Stop()
					timeoutChan = timer.C
				}
				select {
				case <-sub.close:
				case <-timeoutChan:
				case sub.internalCh <- msgToSend:
					sent = true
				}
			}
		}(currentSub, refs[i], message.Topic, topicConfig)
	}
	wg.Wait()
}

func (ps *basePubSub[T]) close() {
	allSubscribers := func() []*Subscriber[T] {
		ps.mu.Lock()
		defer ps.mu.Unlock()
		subs := make([]*Subscriber[T], 0)
		if ps.subscribers != nil {
			for _, topicSubs := range ps.subscribers {
				for _, sub := range topicSubs {
					subs = append(subs, sub)
				}
			}
			ps.subscribers = make(map[string]map[string]*Subscriber[T])
			ps.topicConfigs = make(map[string]TopicConfig)
		}
		return subs
	}()
	var wg sync.WaitGroup
	for _, subInstance := range allSubscribers {
		wg.Add(1)
		go func(s *Subscriber[T]) {
			defer wg.Done()
			s.Unsubscribe()
		}(subInstance)
	}
	wg.Wait()
}

func cleanupOrDrop[T any](data T) {
	if managed, ok := any(data).(ManagedItem[T]); ok {
		managed.Cleanup()
	}
}

// ------------------- Public Type-Safe PubSub API -------------------

// PubSub provides a type-safe, multi-topic publish-subscribe system.
// It manages topic-to-type registration and ensures that publish and subscribe
// operations are type-checked.
type PubSub struct {
	topicSystems map[string]pubSubCore
	mu           sync.RWMutex
}

// NewPubSub creates a new type-safe pub/sub system. This is the main entry
// point for creating a new pub/sub instance.
func NewPubSub() *PubSub {
	return &PubSub{
		topicSystems: make(map[string]pubSubCore),
	}
}

// RegisterTopic associates a topic string with a specific data type. You must
// register a topic's type before publishing or subscribing to it. An optional
// TopicConfig can be provided to set behaviors like message dropping or timeouts.
func RegisterTopic[T any](ps *PubSub, topic string, config ...TopicConfig) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	var typedSystem *basePubSub[T]

	if system, ok := ps.topicSystems[topic]; ok {
		// Topic exists, check if types match
		var okCast bool
		typedSystem, okCast = system.(*basePubSub[T])
		if !okCast {
			var zero T
			return fmt.Errorf("topic '%s' is already registered with a different type than %T", topic, zero)
		}
	} else {
		// Topic does not exist, create it
		typedSystem = newBasePubSub[T]()
		ps.topicSystems[topic] = typedSystem
		logDebug("Registered topic '%s' with type %T", topic, *new(T))
	}

	// Apply config if provided. This allows creating and configuring in one step.
	if len(config) > 0 {
		typedSystem.updateTopic(topic, config[0])
	}

	return nil
}

// UpdateTopic changes or sets the configuration for an already registered topic.
// This is useful for changing behaviors like message dropping or timeouts at runtime.
func (ps *PubSub) UpdateTopic(topic string, config TopicConfig) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	if system, ok := ps.topicSystems[topic]; ok {
		system.updateTopic(topic, config)
	}
}

// Close gracefully shuts down the underlying PubSub system.
func (ps *PubSub) Close() {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	for _, system := range ps.topicSystems {
		system.close()
	}
	ps.topicSystems = make(map[string]pubSubCore) // Clear the map
}

// GetUniqueSubscriberID generates a new unique subscriber ID.
func (ps *PubSub) GetUniqueSubscriberID() string {
	// A single counter is sufficient for all internal pubsub systems.
	return fmt.Sprintf("sub-%d", uniqueIDCounter.Add(1))
}

// Subscribe provides a type-safe way to subscribe to a topic. It returns a
// strongly-typed Subscriber[T] if the topic has been registered with type T.
func Subscribe[T any](ps *PubSub, topic string, subscriberID string, bufferSize int) (*Subscriber[T], error) {
	ps.mu.RLock()
	system, ok := ps.topicSystems[topic]
	ps.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("topic '%s' has not been registered", topic)
	}

	typedSystem, ok := system.(*basePubSub[T])
	if !ok {
		var zero T
		return nil, fmt.Errorf("topic '%s' is registered with a different type than %T", topic, zero)
	}

	sub := typedSystem.subscribe(topic, subscriberID, bufferSize)
	if sub == nil {
		return nil, fmt.Errorf("subscriber ID '%s' already exists for topic '%s'", subscriberID, topic)
	}
	return sub, nil
}

// Publish provides a type-safe way to publish a message.
// It ensures the message's type matches the registered type for the topic.
func Publish[T any](ps *PubSub, message Message[T]) error {
	ps.mu.RLock()
	system, ok := ps.topicSystems[message.Topic]
	ps.mu.RUnlock()

	if !ok {
		cleanupOrDrop(message.Data)
		return fmt.Errorf("topic '%s' has not been registered", message.Topic)
	}

	typedSystem, ok := system.(*basePubSub[T])
	if !ok {
		cleanupOrDrop(message.Data)
		var zero T
		return fmt.Errorf("topic '%s' is registered with a different type than %T", message.Topic, zero)
	}

	typedSystem.publish(message)
	return nil
}

// SendReceive provides a type-safe implementation of a request-response pattern.
// It subscribes to a 'receiveTopic' expecting a response of type ResT, publishes a
// request of type ReqT to a 'sendTopic', and waits for a response.
// It returns the response and true if successful, or the zero value of ResT and false on timeout.
// Note: Both the send and receive topics must be registered with their respective types beforehand.
func SendReceive[ReqT, ResT any](
	ps *PubSub,
	sendTopic string,
	receiveTopic string,
	requestData ReqT,
	timeoutMs int,
) (ResT, bool) {
	// Create a unique subscriber ID for the temporary response listener.
	subID := ps.GetUniqueSubscriberID()

	// Subscribe to the receive topic, expecting the response type.
	sub, err := Subscribe[ResT](ps, receiveTopic, subID, 1)
	if err != nil {
		logDebug("SendReceive: Failed to subscribe to receive topic '%s': %v", receiveTopic, err)
		var zero ResT
		return zero, false
	}
	defer sub.Unsubscribe()

	// Publish the request message.
	err = Publish(ps, Message[ReqT]{Topic: sendTopic, Data: requestData})
	if err != nil {
		// If publishing fails (e.g., topic not registered), we won't get a response.
		logDebug("SendReceive: Failed to publish to send topic '%s': %v", sendTopic, err)
		var zero ResT
		return zero, false
	}

	// Wait for the response or a timeout.
	select {
	case msg, ok := <-sub.Ch:
		if ok {
			// Successfully received a response.
			return msg.Data, true
		}
		// Channel was closed before a message was received.
		logDebug("SendReceive: Subscriber channel for topic '%s' closed before message received.", receiveTopic)
		var zero ResT
		return zero, false
	case <-time.After(time.Millisecond * time.Duration(timeoutMs)):
		logDebug("SendReceive timeout of %dms reached for receive topic '%s'.", timeoutMs, receiveTopic)
		var zero ResT
		return zero, false
	}
}
