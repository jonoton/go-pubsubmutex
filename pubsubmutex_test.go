package pubsubmutex

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- Test Types ---
type UserUpdate struct {
	UserID   int
	NewEmail string
}

type OrderEvent struct {
	OrderID  string
	Status   string
	refCount int32
}

// Implement ManagedItem for OrderEvent for testing reference counting.
func (oe *OrderEvent) Ref() *OrderEvent {
	atomic.AddInt32(&oe.refCount, 1)
	logDebug("Ref() called for OrderID %s, new count: %d", oe.OrderID, oe.refCount)
	return oe // Return same pointer for simplicity in this test
}

func (oe *OrderEvent) Cleanup() {
	newCount := atomic.AddInt32(&oe.refCount, -1)
	logDebug("Cleanup() called for OrderID %s, new count: %d", oe.OrderID, newCount)
}

func (oe *OrderEvent) GetRefCount() int32 {
	return atomic.LoadInt32(&oe.refCount)
}

// --- Tests for PubSub (Type-Safe Wrapper) ---

func Test_RegisterAndSubscribeSuccess(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	RegisterTopic[UserUpdate](ps, "users")

	sub, err := Subscribe[UserUpdate](ps, "users", "sub1", 10)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	if sub == nil {
		t.Fatal("Subscribe returned nil subscriber")
	}

	t.Log("Successfully registered and subscribed to a typed topic.")
}

func Test_RegisterTopicWithWrongType(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	// Register topic with UserUpdate type first.
	err := RegisterTopic[UserUpdate](ps, "events")
	if err != nil {
		t.Fatalf("Initial topic registration failed: %v", err)
	}

	// Attempt to register the same topic with a different type.
	err = RegisterTopic[OrderEvent](ps, "events")
	if err == nil {
		t.Fatal("Expected an error when re-registering a topic with a different type, but got nil")
	}
	t.Logf("Correctly received error when re-registering topic with wrong type: %v", err)
}

func Test_SubscribeToUnregisteredTopic(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	_, err := Subscribe[UserUpdate](ps, "unregistered.topic", "sub1", 10)
	if err == nil {
		t.Fatal("Expected an error when subscribing to an unregistered topic, but got nil")
	}
	t.Logf("Correctly received error for unregistered topic: %v", err)
}

func Test_SubscribeWithWrongType(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	RegisterTopic[OrderEvent](ps, "orders")

	_, err := Subscribe[UserUpdate](ps, "orders", "sub1", 10)
	if err == nil {
		t.Fatal("Expected an error when subscribing with the wrong type, but got nil")
	}
	t.Logf("Correctly received error for wrong type subscription: %v", err)
}

func Test_PublishAndReceive(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	topic := "user.updates"
	RegisterTopic[UserUpdate](ps, topic)

	sub, err := Subscribe[UserUpdate](ps, topic, "user-service", 10)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	msgData := UserUpdate{UserID: 123, NewEmail: "test@example.com"}
	done := make(chan bool)
	ready := make(chan struct{})

	go func() {
		close(ready) // Signal that the goroutine is ready to receive.
		msg := <-sub.Ch
		if msg.Data.UserID != 123 || msg.Data.NewEmail != "test@example.com" {
			t.Errorf("Received incorrect data: got %+v, want %+v", msg.Data, msgData)
		}
		done <- true
	}()

	<-ready // Wait for the goroutine to be ready before publishing.

	err = Publish(ps, Message[UserUpdate]{Topic: topic, Data: msgData})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	<-done
	t.Log("Successfully published and received a type-safe message.")
}

func Test_PublishWithWrongType(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	RegisterTopic[UserUpdate](ps, "users")

	wrongMsg := Message[OrderEvent]{Topic: "users", Data: OrderEvent{OrderID: "xyz"}}
	err := Publish(ps, wrongMsg)

	if err == nil {
		t.Fatal("Expected an error when publishing with the wrong type, but got nil")
	}
	t.Logf("Correctly received error for wrong type publish: %v", err)
}

func Test_PublishToMultipleSubs_NonManaged(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	topic := "alerts"
	RegisterTopic[string](ps, topic)

	sub1, _ := Subscribe[string](ps, topic, "sub1", 10)
	sub2, _ := Subscribe[string](ps, topic, "sub2", 10)
	sub3, _ := Subscribe[string](ps, topic, "sub3", 10)

	var wg sync.WaitGroup
	wg.Add(3)

	process := func(sub *Subscriber[string], expectedMsg string) {
		defer wg.Done()
		msg := <-sub.Ch
		if msg.Data != expectedMsg {
			t.Errorf("Subscriber %s received incorrect message. Got '%s', want '%s'", sub.ID, msg.Data, expectedMsg)
		}
	}

	go process(sub1, "system critical")
	go process(sub2, "system critical")
	go process(sub3, "system critical")

	err := Publish(ps, Message[string]{Topic: topic, Data: "system critical"})
	if err != nil {
		t.Fatalf("Publish failed unexpectedly: %v", err)
	}

	wg.Wait()
	t.Log("Successfully published a non-managed item to multiple subscribers.")
}

func Test_ManagedItemRefCount(t *testing.T) {
	SetDebug(true)
	defer SetDebug(false)

	ps := NewPubSub()
	defer ps.Close()

	topic := "managed.orders"
	RegisterTopic[*OrderEvent](ps, topic)

	sub1, _ := Subscribe[*OrderEvent](ps, topic, "sub1", 10)
	sub2, _ := Subscribe[*OrderEvent](ps, topic, "sub2", 10)
	sub3, _ := Subscribe[*OrderEvent](ps, topic, "sub3", 1) // Small buffer to test dropping

	if sub1 == nil || sub2 == nil || sub3 == nil {
		t.Fatal("A subscriber was nil, setup failed.")
	}

	// Initial refCount is 1 because we create it here.
	order := &OrderEvent{OrderID: "order-123", refCount: 1}

	// Use a WaitGroup to ensure all subscribers have received the message before proceeding.
	var receiveWg sync.WaitGroup
	receiveWg.Add(3)
	go func() { defer receiveWg.Done(); <-sub1.Ch }()
	go func() { defer receiveWg.Done(); <-sub2.Ch }()
	go func() { defer receiveWg.Done(); <-sub3.Ch }()

	// Publish to 3 subscribers. The original reference is now owned by the pubsub system.
	// Ref() should be called twice for the other two subscribers.
	// Expected refCount after publish = 1 (original) + 2 (refs) = 3
	err := Publish(ps, Message[*OrderEvent]{Topic: topic, Data: order})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	receiveWg.Wait() // Wait for all subscribers to receive the message.

	if count := order.GetRefCount(); count != 3 {
		t.Fatalf("Expected ref count of 3 after publishing to 3 subscribers, but got %d", count)
	}
	t.Log("Ref count is correctly 3 after publishing.")

	// The message has been "delivered" to the client goroutines started above.
	// Now, the client code is responsible for calling Cleanup(). Let's simulate that.
	order.Cleanup() // Client 1 is done with it.
	if count := order.GetRefCount(); count != 2 {
		t.Fatalf("Expected ref count of 2 after client 1 cleans up, but got %d", count)
	}
	t.Log("Ref count is correctly 2 after client 1 cleanup.")

	order.Cleanup() // Client 2 is done with it.
	if count := order.GetRefCount(); count != 1 {
		t.Fatalf("Expected ref count of 1 after client 2 cleans up, but got %d", count)
	}
	t.Log("Ref count is correctly 1 after client 2 cleanup.")

	order.Cleanup() // Client 3 is done with it.
	if count := order.GetRefCount(); count != 0 {
		t.Fatalf("Expected ref count of 0 after all clients cleanup, but got %d", count)
	}
	t.Log("Ref count is correctly 0 after all clients cleanup.")

	// Drop a message for sub3 by filling its buffer.
	t.Run("DropMessageCleanup", func(t *testing.T) {
		// To reliably test dropping, we need to ensure sub3's internal buffer is full.
		// First, publish a "blocker" message. sub3's deliverMessages will pick it up
		// and block on its public Ch, leaving its internal buffer empty.
		blocker := &OrderEvent{OrderID: "blocker", refCount: 1}
		Publish(ps, Message[*OrderEvent]{Topic: topic, Data: blocker})

		// Now, publish a "filler" message. This will fill sub3's internal buffer of size 1.
		filler := &OrderEvent{OrderID: "filler", refCount: 1}
		Publish(ps, Message[*OrderEvent]{Topic: topic, Data: filler})

		// Now, publish the message we expect sub3 to drop.
		// It will be sent to sub1 and sub2 successfully.
		orderToDrop := &OrderEvent{OrderID: "order-to-drop", refCount: 1}
		ps.UpdateTopic(topic, TopicConfig{AllowDropping: true})
		Publish(ps, Message[*OrderEvent]{Topic: topic, Data: orderToDrop})

		// The publish call for orderToDrop will spawn 3 goroutines.
		// The one for sub3 should drop and call Cleanup().
		// The ones for sub1 and sub2 will succeed.
		// Initial refcount = 1. Two more refs are made. Total = 3. One is cleaned up. Final should be 2.

		// Poll for the cleanup to occur, as it's asynchronous.
		cleaned := false
		for i := 0; i < 100; i++ { // Poll for up to 1 second.
			if orderToDrop.GetRefCount() == 2 {
				cleaned = true
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		if !cleaned {
			t.Fatalf("Expected ref count of 2 for partially dropped message, but got %d", orderToDrop.GetRefCount())
		}
		t.Log("Ref count is correctly 2 for message partially dropped.")

		// Clean up the remaining references held by sub1 and sub2 to avoid leaking in the test.
		orderToDrop.Cleanup()
		orderToDrop.Cleanup()
		if orderToDrop.GetRefCount() != 0 {
			t.Fatalf("Failed to cleanup remaining references, count is %d", orderToDrop.GetRefCount())
		}
	})
}

// -----------------------------------------------------------------------------
// Additional Core API Tests
// -----------------------------------------------------------------------------

func Test_Unsubscribe(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	topic := "unsub_topic"
	RegisterTopic[string](ps, topic)
	sub, err := Subscribe[string](ps, topic, "sub_to_unsub", 5)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Publish and ensure message is received before unsubscribing
	done := make(chan bool)
	go func() {
		<-sub.Ch
		done <- true
	}()
	Publish(ps, Message[string]{Topic: topic, Data: "First Message"})
	<-done

	// Unsubscribe
	sub.Unsubscribe()

	// This message should not be delivered
	Publish(ps, Message[string]{Topic: topic, Data: "Second Message (after unsub)"})

	// Check that the channel is closed and no more messages arrive
	select {
	case msg, ok := <-sub.Ch:
		if ok {
			t.Errorf("Received message '%s' after unsubscription", msg.Data)
		} else {
			t.Log("Channel closed as expected after unsubscribe.")
		}
	case <-time.After(200 * time.Millisecond):
		t.Log("No message received after unsubscribe, as expected.")
	}
}

func Test_TopicConfigAllowDropping(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	topic := "drop_topic"
	// Register the topic with the AllowDropping config
	RegisterTopic[int](ps, topic, TopicConfig{AllowDropping: true})

	// Subscriber with a small buffer and a slow consumer
	sub, _ := Subscribe[int](ps, topic, "dropper_sub", 1)
	var receivedCount atomic.Int32
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range sub.Ch {
			receivedCount.Add(1)
			time.Sleep(50 * time.Millisecond) // Simulate slow processing
		}
	}()

	// Publish many messages quickly. Most should be dropped.
	numToPublish := 100
	for i := 0; i < numToPublish; i++ {
		Publish(ps, Message[int]{Topic: topic, Data: i})
	}

	sub.Unsubscribe() // Unsubscribe to close the channel and stop the consumer goroutine.
	wg.Wait()         // Wait for consumer to finish.

	finalCount := receivedCount.Load()
	if finalCount == 0 {
		t.Errorf("No messages received, expected at least 1.")
	}
	if finalCount >= int32(numToPublish) {
		t.Errorf("Received all messages (%d), but expected some to be dropped.", finalCount)
	}
	t.Logf("Published %d, slow consumer received %d. Dropping works as expected.", numToPublish, finalCount)
}

func Test_TopicConfigPublishTimeout(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	topic := "timeout_topic"
	timeoutDuration := 100 * time.Millisecond
	RegisterTopic[string](ps, topic, TopicConfig{AllowDropping: false, PublishTimeout: timeoutDuration})

	// Subscriber with a full buffer that is not being read.
	sub, _ := Subscribe[string](ps, topic, "timeout-sub", 1)
	Publish(ps, Message[string]{Topic: topic, Data: "blocker"}) // To block deliverMessages
	Publish(ps, Message[string]{Topic: topic, Data: "filler"})  // To fill internalCh

	// This publish call should block and then time out.
	startTime := time.Now()
	Publish(ps, Message[string]{Topic: topic, Data: "should be dropped"})
	elapsed := time.Since(startTime)

	if elapsed < timeoutDuration {
		t.Errorf("Publish returned too quickly (%v), expected to block for at least %v", elapsed, timeoutDuration)
	} else {
		t.Logf("Publish correctly blocked for ~%v due to timeout.", elapsed)
	}

	// Unsubscribe to clean up.
	sub.Unsubscribe()
}

func Test_Close(t *testing.T) {
	ps := NewPubSub()

	topic1 := "close_topic_1"
	topic2 := "close_topic_2"
	RegisterTopic[string](ps, topic1)
	RegisterTopic[string](ps, topic2)

	sub1, _ := Subscribe[string](ps, topic1, "sub1", 5)
	sub2, _ := Subscribe[string](ps, topic2, "sub2", 5)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range sub1.Ch {
		}
	}()
	go func() {
		defer wg.Done()
		for range sub2.Ch {
		}
	}()

	Publish(ps, Message[string]{Topic: topic1, Data: "message 1"})
	Publish(ps, Message[string]{Topic: topic2, Data: "message 2"})

	ps.Close()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Log("All subscriber goroutines exited gracefully after Close().")
	case <-time.After(1 * time.Second):
		t.Fatal("Subscribers did not shut down correctly after Close().")
	}

	// Verify internal state is cleared.
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	if len(ps.topicSystems) != 0 {
		t.Errorf("topicSystems map not empty after Close. Contains %d entries.", len(ps.topicSystems))
	}
}

// -----------------------------------------------------------------------------
// New Tests for ReadMessages, GetUniqueSubscriberID, and SendReceive
// -----------------------------------------------------------------------------

func Test_ReadMessages(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	topic := "read_messages_topic"
	RegisterTopic[string](ps, topic)
	sub, err := Subscribe[string](ps, topic, "reader", 5)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	var receivedMessages []string
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(2) // Expecting 2 messages

	// The handler will be called by ReadMessages for each message.
	handler := func(msg Message[string]) {
		mu.Lock()
		receivedMessages = append(receivedMessages, msg.Data)
		mu.Unlock()
		wg.Done()
	}

	// Start ReadMessages in a goroutine. It will block until the subscriber's channel is closed.
	go sub.ReadMessages(handler)

	// Publish two messages.
	Publish(ps, Message[string]{Topic: topic, Data: "message one"})
	Publish(ps, Message[string]{Topic: topic, Data: "message two"})

	// Wait for the handler to process both messages.
	wg.Wait()

	// Unsubscribe to stop the ReadMessages loop.
	sub.Unsubscribe()

	mu.Lock()
	defer mu.Unlock()
	if len(receivedMessages) != 2 {
		t.Errorf("Expected to receive 2 messages, but got %d", len(receivedMessages))
	}
	if receivedMessages[0] != "message one" || receivedMessages[1] != "message two" {
		t.Errorf("Received incorrect messages: %v", receivedMessages)
	}
	t.Log("ReadMessages correctly processed all messages.")
}

func Test_GetUniqueSubscriberID(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	numIDs := 1000
	idMap := make(map[string]bool, numIDs)
	var mu sync.Mutex
	var wg sync.WaitGroup

	wg.Add(numIDs)
	for i := 0; i < numIDs; i++ {
		go func() {
			defer wg.Done()
			id := ps.GetUniqueSubscriberID()
			mu.Lock()
			if _, exists := idMap[id]; exists {
				t.Errorf("Generated duplicate subscriber ID: %s", id)
			}
			idMap[id] = true
			mu.Unlock()
		}()
	}

	wg.Wait()
	if len(idMap) != numIDs {
		t.Errorf("Expected %d unique IDs, but got %d", numIDs, len(idMap))
	}
	t.Logf("Successfully generated %d unique subscriber IDs.", numIDs)
}

func Test_SendReceive(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	type Ping struct{ Val string }
	type Pong struct{ Val string }

	reqTopic := "ping.req"
	resTopic := "ping.res"
	RegisterTopic[Ping](ps, reqTopic)
	RegisterTopic[Pong](ps, resTopic)

	// Start a "server" that listens for pings and responds with pongs.
	serverSub, _ := Subscribe[Ping](ps, reqTopic, "server", 1)
	go func() {
		for msg := range serverSub.Ch {
			if msg.Data.Val == "Ping!" {
				Publish(ps, Message[Pong]{Topic: resTopic, Data: Pong{Val: "Pong!"}})
			}
		}
	}()
	defer serverSub.Unsubscribe()

	// Test success case
	t.Run("Success", func(t *testing.T) {
		res, ok := SendReceive[Ping, Pong](ps, reqTopic, resTopic, Ping{Val: "Ping!"}, 500)
		if !ok {
			t.Fatal("SendReceive failed, expected successful response but got !ok")
		}
		if res.Val != "Pong!" {
			t.Errorf("Expected response 'Pong!', got '%s'", res.Val)
		}
		t.Log("Successfully received correct response.")
	})

	// Test timeout case
	t.Run("Timeout", func(t *testing.T) {
		// Publish to a different topic that has no listener for the response
		_, ok := SendReceive[Ping, Pong](ps, "other.topic", resTopic, Ping{Val: "Ping!"}, 100)
		if ok {
			t.Fatal("SendReceive unexpectedly succeeded, expected timeout")
		}
		t.Log("Correctly timed out when no response was sent.")
	})
}

func Test_ManagedItemCleanupOnUnsubscribe(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	topic := "cleanup-on-unsub"
	RegisterTopic[*OrderEvent](ps, topic)
	sub, _ := Subscribe[*OrderEvent](ps, topic, "worker", 5)

	order1 := &OrderEvent{OrderID: "order-unsub-1", refCount: 1}
	order2 := &OrderEvent{OrderID: "order-unsub-2", refCount: 1}

	Publish(ps, Message[*OrderEvent]{Topic: topic, Data: order1})
	Publish(ps, Message[*OrderEvent]{Topic: topic, Data: order2})

	// Give messages time to get into the internal buffer.
	// This is one of the few cases where a small sleep is acceptable in a test,
	// to allow asynchronous operations to progress before we initiate cleanup.
	time.Sleep(50 * time.Millisecond)

	// Unsubscribe. This is a blocking call and should handle cleanup.
	sub.Unsubscribe()

	if count := order1.GetRefCount(); count != 0 {
		t.Errorf("Expected ref count of 0 for order1 after unsubscribe, but got %d", count)
	}
	if count := order2.GetRefCount(); count != 0 {
		t.Errorf("Expected ref count of 0 for order2 after unsubscribe, but got %d", count)
	}
	t.Log("Successfully cleaned up buffered messages on Unsubscribe.")
}

func Test_ManagedItemCleanupOnClose(t *testing.T) {
	ps := NewPubSub()

	topic1 := "cleanup-on-close-1"
	topic2 := "cleanup-on-close-2"
	RegisterTopic[*OrderEvent](ps, topic1)
	RegisterTopic[UserUpdate](ps, topic2) // A non-managed item topic

	sub1, _ := Subscribe[*OrderEvent](ps, topic1, "worker1", 5)
	sub2, _ := Subscribe[UserUpdate](ps, topic2, "worker2", 5)

	order1 := &OrderEvent{OrderID: "order-close-1", refCount: 1}
	order2 := &OrderEvent{OrderID: "order-close-2", refCount: 1}

	// Publish managed items that will remain in the buffer
	Publish(ps, Message[*OrderEvent]{Topic: topic1, Data: order1})
	Publish(ps, Message[*OrderEvent]{Topic: topic1, Data: order2})
	// Publish a non-managed item
	Publish(ps, Message[UserUpdate]{Topic: topic2, Data: UserUpdate{UserID: 99}})

	// Give a moment for publish to complete
	time.Sleep(50 * time.Millisecond)

	// Close the PubSub system. This is a blocking call and should wait for all cleanup.
	ps.Close()

	// After Close() returns, all cleanup should be finished.
	if count := order1.GetRefCount(); count != 0 {
		t.Errorf("Expected ref count of 0 for order1 after Close, but got %d", count)
	}
	if count := order2.GetRefCount(); count != 0 {
		t.Errorf("Expected ref count of 0 for order2 after Close, but got %d", count)
	}
	t.Log("Successfully cleaned up buffered messages on PubSub.Close().")

	// Check that the goroutines for the subscribers have exited by trying to read from their channels.
	// A read from a closed channel returns immediately with the zero value.
	select {
	case _, ok := <-sub1.Ch:
		if ok {
			t.Error("sub1 channel was not closed after PubSub.Close()")
		}
	default:
		// To avoid this test being flaky, we give a short timeout.
		// A read from a closed channel is non-blocking. If it blocks, it means it wasn't closed.
		timer := time.NewTimer(50 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-sub1.Ch:
			// Still ok, channel is closed and drained.
		case <-timer.C:
			t.Error("Reading from sub1.Ch blocked, channel was not closed")
		}
	}
	select {
	case _, ok := <-sub2.Ch:
		if ok {
			t.Error("sub2 channel was not closed after PubSub.Close()")
		}
	default:
		timer := time.NewTimer(50 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-sub2.Ch:
		case <-timer.C:
			t.Error("Reading from sub2.Ch blocked, channel was not closed")
		}
	}
}
