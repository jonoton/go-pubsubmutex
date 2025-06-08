package pubsubmutex

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Helper to enable debug logging for tests if needed
func init() {
	SetDebug(false) // Uncomment this line to see debug logs during tests
}

// waitForMessages attempts to read 'count' messages from a channel within a timeout.
// Returns the received messages and a boolean indicating if 'count' messages were received.
func waitForMessages(t *testing.T, ch <-chan Message, expectedCount int, timeout time.Duration) ([]Message, bool) {
	t.Helper()
	receivedMessages := make([]Message, 0, expectedCount)
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for i := 0; i < expectedCount; i++ {
		select {
		case msg, ok := <-ch:
			if !ok {
				t.Logf("Channel closed unexpectedly after receiving %d of %d messages.", len(receivedMessages), expectedCount)
				return receivedMessages, false
			}
			receivedMessages = append(receivedMessages, msg)
		case <-timer.C:
			t.Logf("Timeout waiting for messages. Expected %d, Received %d. Timeout: %s", expectedCount, len(receivedMessages), timeout)
			return receivedMessages, false
		}
	}
	return receivedMessages, true
}

// TestNewPubSub tests the creation of a new PubSub system.
func TestNewPubSub(t *testing.T) {
	ps := NewPubSub()
	if ps == nil {
		t.Fatal("NewPubSub returned nil")
	}
	if ps.subscribers == nil {
		t.Error("subscribers map is nil")
	}
	if ps.topicConfigs == nil {
		t.Error("topicConfigs map is nil")
	}
	t.Log("NewPubSub tested successfully.")
}

// TestSubscribePublish tests basic subscribe and publish functionality.
func TestSubscribePublish(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	topic := "test_topic"
	subID1 := "sub1"
	subID2 := "sub2"
	messageData1 := "Hello, World!"
	messageData2 := 123
	messageData3 := true

	sub1 := ps.Subscribe(topic, subID1, 10)
	sub2 := ps.Subscribe(topic, subID2, 10)

	if sub1 == nil || sub2 == nil {
		t.Fatal("Subscription returned nil")
	}

	ps.Publish(Message{Topic: topic, Data: messageData1})
	ps.Publish(Message{Topic: topic, Data: messageData2})
	ps.Publish(Message{Topic: topic, Data: messageData3})

	// Wait for messages
	received1, ok1 := waitForMessages(t, sub1.Ch, 3, 500*time.Millisecond)
	received2, ok2 := waitForMessages(t, sub2.Ch, 3, 500*time.Millisecond)

	if !ok1 || len(received1) != 3 {
		t.Errorf("Subscriber 1 did not receive all messages. Got %d, Expected 3", len(received1))
	} else {
		if received1[0].Data != messageData1 || received1[1].Data != messageData2 || received1[2].Data != messageData3 {
			t.Errorf("Subscriber 1 received incorrect messages: %+v", received1)
		}
	}

	if !ok2 || len(received2) != 3 {
		t.Errorf("Subscriber 2 did not receive all messages. Got %d, Expected 3", len(received2))
	} else {
		if received2[0].Data != messageData1 || received2[1].Data != messageData2 || received2[2].Data != messageData3 {
			t.Errorf("Subscriber 2 received incorrect messages: %+v", received2)
		}
	}
	t.Log("Subscribe/Publish tested successfully.")
}

// TestUnsubscribe tests that unsubscribed clients no longer receive messages.
func TestUnsubscribe(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	topic := "unsub_topic"
	subID := "sub_to_unsub"
	sub := ps.Subscribe(topic, subID, 5)
	if sub == nil {
		t.Fatal("Subscription returned nil")
	}

	ps.Publish(Message{Topic: topic, Data: "First Message"})

	// Verify first message received
	_, ok := waitForMessages(t, sub.Ch, 1, 200*time.Millisecond) // Increased timeout slightly
	if !ok {
		t.Fatal("Subscriber did not receive initial message.")
	}

	ps.CleanupSub(sub)

	ps.Publish(Message{Topic: topic, Data: "Second Message (after unsub)"})

	// Try to read more messages from sub.Ch. It should be closed or empty.
	// waitForMessages expects a certain count. Here, we expect 0 more *new* messages.
	// The channel should be closed by CleanupSub's logic eventually.
	// A more direct check is to see if reading from sub.Ch blocks then returns !ok.
	select {
	case msg, chanOk := <-sub.Ch:
		if chanOk {
			t.Errorf("Subscriber received message '%v' after unsubscription and CleanupSub.", msg)
		} else {
			t.Log("Subscriber channel closed as expected after CleanupSub.")
		}
	case <-time.After(200 * time.Millisecond): // Increased timeout
		t.Log("Subscriber channel did not yield messages after unsubscription (timed out), as expected if empty and not yet closed by test end.")
		// This case is fine if the channel is empty but not yet closed by the time this select hits.
		// The key is that no *new* message ("Second Message") should arrive.
	}

	ps.mu.RLock()
	defer ps.mu.RUnlock()
	if topicSubs, topicExists := ps.subscribers[topic]; topicExists {
		if _, subExists := topicSubs[subID]; subExists {
			t.Errorf("Subscriber '%s' still found in pubsub map after CleanupSub.", subID)
		}
	}
	t.Log("Unsubscribe tested successfully.")
}

// TestSendReceiveSuccess tests the SendReceive happy path.
func TestSendReceiveSuccess(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	sendTopic := "request"
	receiveTopic := "response"
	requestMsg := "Ping"
	responseMsg := "Pong"

	serverSubID := "server"
	serverSub := ps.Subscribe(sendTopic, serverSubID, 1)
	if serverSub == nil {
		t.Fatalf("Failed to subscribe serverSub")
	}
	defer ps.CleanupSub(serverSub) // Ensure serverSub is cleaned up

	go func() {
		for msg := range serverSub.Ch {
			if msg.Data == requestMsg {
				ps.Publish(Message{Topic: receiveTopic, Data: responseMsg})
			}
		}
	}()

	t.Log("Calling SendReceive for 'Ping'.")
	result := ps.SendReceive(sendTopic, receiveTopic, requestMsg, 500) // 500ms timeout
	if result != responseMsg {
		t.Errorf("SendReceive failed. Expected '%s', Got '%v'", responseMsg, result)
	}
	t.Log("SendReceive success tested successfully.")
}

// TestSendReceiveTimeout tests the SendReceive timeout scenario.
func TestSendReceiveTimeout(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	sendTopic := "request_timeout"
	receiveTopic := "response_timeout"
	requestMsg := "Ping"

	t.Log("Calling SendReceive with a short timeout, expecting nil result.")
	timeoutMs := 100
	startTime := time.Now()
	result := ps.SendReceive(sendTopic, receiveTopic, requestMsg, timeoutMs)
	elapsed := time.Since(startTime)

	if result != nil {
		t.Errorf("SendReceive unexpectedly received a result: %v", result)
	}
	// Check if elapsed time is around the timeout value.
	// It could be slightly less due to scheduling, or slightly more.
	// A common check is that it's not significantly less.
	if elapsed < time.Duration(timeoutMs)*time.Millisecond*8/10 { // e.g. within 80% of timeout
		t.Errorf("SendReceive returned too quickly. Expected at least ~%dms, Got %s", timeoutMs, elapsed)
	}
	t.Logf("SendReceive timeout tested successfully. Elapsed time: %s", elapsed)
}

func setupFullInternalChannel(t *testing.T, ps *PubSub, topic string, subID string, bufferSize int) *Subscriber {
	t.Helper()
	if bufferSize < 1 {
		// This setup requires a buffer to fill.
		// The pubsub code might ensure bufferSize > 0. If not, this test logic needs adjustment.
		// For current pubsub, internalCh is `make(chan Message, bufferSize)`, so 0 is possible but changes test logic.
		// Let's assume bufferSize >= 1 for these tests.
		t.Fatalf("Buffer size must be at least 1 for this test setup helper.")
	}

	sub := ps.Subscribe(topic, subID, bufferSize)
	if sub == nil {
		t.Fatalf("Failed to subscribe in setupFullInternalChannel for %s", subID)
	}

	// 1. Publish one message to make deliverMessages read it and block on the unread sub.Ch
	ps.Publish(Message{Topic: topic, Data: fmt.Sprintf("BlockerMsg for %s", subID)})
	// Give a moment for deliverMessages to pick up the blocker message and block on sub.Ch
	// This is a bit heuristic; ideally, we'd have a more deterministic way to know deliverMessages is blocked.
	// For tests, a small sleep is often pragmatic if it makes them pass reliably.
	time.Sleep(50 * time.Millisecond) // Adjust if needed

	// 2. Publish 'bufferSize' messages to fill internalCh
	// (since deliverMessages took the first one, internalCh is empty before these)
	for i := 0; i < bufferSize; i++ {
		ps.Publish(Message{Topic: topic, Data: fmt.Sprintf("FillMsg %d for %s", i, subID)})
	}
	// Give a moment for these messages to queue up in internalCh
	time.Sleep(50 * time.Millisecond) // Adjust if needed

	return sub
}

// TestTopicConfigAllowDropping tests topics configured to allow message dropping.
func TestTopicConfigAllowDropping(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	topic := "drop_topic"
	ps.CreateTopic(topic, TopicConfig{AllowDropping: true})

	bufferSize := 1
	sub := ps.Subscribe(topic, "dropper_sub", bufferSize)
	if sub == nil {
		t.Fatal("Subscription returned nil")
	}
	var receivedCount atomic.Int32

	// Start a slow consumer
	var consumerWg sync.WaitGroup
	consumerWg.Add(1)
	go func() {
		defer consumerWg.Done()
		for range sub.Ch {
			receivedCount.Add(1)
			time.Sleep(50 * time.Millisecond) // Simulate slow processing
		}
	}()

	// Publish many messages quickly
	// The first 'bufferSize' messages might get into internalCh.
	// The next one might get picked by deliverMessages if it's fast enough to take the first and block on sub.Ch.
	// Then subsequent ones should hit a full internalCh (as deliverMessages is slow) and drop.
	numMessagesToPublish := 100
	for i := 0; i < numMessagesToPublish; i++ {
		ps.Publish(Message{Topic: topic, Data: fmt.Sprintf("Msg %d", i)})
	}

	// Wait for a period to allow processing and dropping
	// Enough time for the slow consumer to process a few, but not all if dropping worked.
	time.Sleep(time.Duration(bufferSize+5) * 50 * time.Millisecond * 2) // Wait longer than just a few msgs

	// Signal consumer to stop by cleaning up the sub, which closes sub.Ch via deliverMessages
	ps.CleanupSub(sub)
	consumerWg.Wait() // Wait for consumer to finish processing

	finalReceived := receivedCount.Load()

	if finalReceived == 0 && numMessagesToPublish > 0 {
		t.Errorf("No messages were received. Expected some (at least bufferSize), but less than total due to dropping.")
	}
	if finalReceived >= int32(numMessagesToPublish) { // Use >= in case of race or exact delivery
		t.Errorf("All %d messages appear to have been received (got %d). Expected some to be dropped.", numMessagesToPublish, finalReceived)
	}
	if finalReceived > 0 && finalReceived < int32(numMessagesToPublish) {
		t.Logf("Topic configured for dropping: Published %d, Received %d. Correctly dropped some messages.", numMessagesToPublish, finalReceived)
	}
	t.Log("TopicConfig (AllowDropping) tested successfully.")
}

// TestTopicConfigPublishTimeoutBlocking tests topics configured for blocking (no timeout, PublishTimeout=0).
func TestTopicConfigPublishTimeoutBlocking(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	topic := "block_topic_0_timeout"
	bufferSize := 1
	ps.CreateTopic(topic, TopicConfig{AllowDropping: false, PublishTimeout: 0}) // Indefinite block

	sub := setupFullInternalChannel(t, ps, topic, "block_sub_0", bufferSize)

	// Attempt to publish one more message (bufferSize + 2 overall); this should block indefinitely.
	donePublishingBlockingMsg := make(chan struct{})
	blockingMsgData := "Msg Block (should block then succeed)"
	go func() {
		t.Logf("Attempting to publish blocking message: %s", blockingMsgData)
		ps.Publish(Message{Topic: topic, Data: blockingMsgData})
		t.Logf("Blocking message published: %s", blockingMsgData)
		close(donePublishingBlockingMsg)
	}()

	// Check that it's actually blocking
	select {
	case <-donePublishingBlockingMsg:
		t.Fatal("Publisher did NOT block for PublishTimeout=0 when internalCh was full.")
	case <-time.After(200 * time.Millisecond): // Increased from 100ms
		t.Log("Publisher correctly blocked as expected.")
	}

	// Now, consume one message from sub.Ch. This unblocks deliverMessages,
	// which then empties one slot from internalCh, allowing the blocked publish to proceed.
	t.Log("Consuming one message from sub.Ch to unblock publisher...")
	msgs, ok := waitForMessages(t, sub.Ch, 1, 200*time.Millisecond)
	if !ok || len(msgs) == 0 {
		t.Fatal("Failed to consume a message from sub.Ch to unblock publisher.")
	}
	t.Logf("Consumed message: %v", msgs[0].Data) // Should be "BlockerMsg for block_sub_0"

	// The blocked publisher should now complete.
	select {
	case <-donePublishingBlockingMsg:
		t.Log("Publisher successfully unblocked and published the message.")
	case <-time.After(1 * time.Second): // Increased timeout
		t.Fatal("Publisher remained blocked even after a message was consumed from sub.Ch.")
	}

	// Consume the remaining (bufferSize) messages from internalCh plus the one that was just unblocked.
	// Total messages initially in internalCh via setup = bufferSize ("FillMsg"s)
	// Message that was blocked and now published = 1
	// Total to consume now = bufferSize + 1
	expectedRemainingCount := bufferSize + 1
	finalMsgs, ok := waitForMessages(t, sub.Ch, expectedRemainingCount, time.Duration(expectedRemainingCount)*200*time.Millisecond)
	if !ok || len(finalMsgs) != expectedRemainingCount {
		t.Fatalf("Did not receive all expected messages after unblocking. Got %d, Expected %d. Received: %+v", len(finalMsgs), expectedRemainingCount, finalMsgs)
	}
	// Check if the last message received is the one that was blocking
	foundBlockingMsg := false
	for _, msg := range finalMsgs {
		if msg.Data == blockingMsgData {
			foundBlockingMsg = true
			break
		}
	}
	if !foundBlockingMsg {
		t.Errorf("The message '%s' that was supposed to be blocked and then published was not found in the final received messages.", blockingMsgData)
	}

	t.Log("TopicConfig (PublishTimeout=0 / Blocking) tested successfully.")
}

// TestTopicConfigPublishTimeout tests topics configured with a specific publish timeout.
func TestTopicConfigPublishTimeout(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	topic := "timeout_topic_specific"
	publishTimeoutDuration := 150 * time.Millisecond // Increased slightly
	bufferSize := 1
	ps.CreateTopic(topic, TopicConfig{AllowDropping: false, PublishTimeout: publishTimeoutDuration})

	sub := setupFullInternalChannel(t, ps, topic, "timeout_sub_specific", bufferSize)

	// Attempt to publish one more message; this should hit the full internalCh and timeout.
	publishDone := make(chan bool, 1) // true if timed out as expected, false otherwise
	msgToTimeout := "Msg that Should Timeout (specific)"

	go func() {
		startTime := time.Now()
		ps.Publish(Message{Topic: topic, Data: msgToTimeout})
		elapsed := time.Since(startTime)
		// Check if elapsed is around publishTimeoutDuration.
		// It should take AT LEAST publishTimeoutDuration if it timed out.
		// If it sent successfully (unexpectedly), elapsed would be short.
		if elapsed >= publishTimeoutDuration-(50*time.Millisecond) { // Allow some leeway
			logDebug("Publisher goroutine: Publish call took %s, timeout was %s. Reporting timeout occurred.", elapsed, publishTimeoutDuration)
			publishDone <- true // Indicates it likely timed out or took long enough
		} else {
			logDebug("Publisher goroutine: Publish call took %s, timeout was %s. Reporting NO timeout.", elapsed, publishTimeoutDuration)
			publishDone <- false // Indicates it returned too fast (message likely sent)
		}
	}()

	// Wait for the publish goroutine to complete and report.
	didItTimeout := false
	select {
	case result := <-publishDone:
		didItTimeout = result
		if result {
			t.Logf("Publish call completed and took expected duration for timeout (%s).", publishTimeoutDuration)
		} else {
			t.Errorf("Publish call completed too quickly, suggesting it did not time out as expected.")
		}
	case <-time.After(publishTimeoutDuration + 200*time.Millisecond): // Increased wait
		t.Fatal("Publish goroutine did not complete in expected time.")
	}

	if !didItTimeout {
		t.Error("Expected publish operation to timeout, but it seems it did not (or reported incorrectly).")
	}

	// Verify only the messages from setupFullInternalChannel are in sub.Ch
	// The "BlockerMsg" was consumed by deliverMessages and is stuck.
	// The "FillMsg"s (bufferSize of them) are in internalCh.
	// The "Msg that Should Timeout" should not have been delivered to internalCh.
	// So, sub.Ch should eventually yield bufferSize messages if we read from it.
	msgsReceived, ok := waitForMessages(t, sub.Ch, bufferSize, time.Duration(bufferSize+1)*100*time.Millisecond)
	if !ok && bufferSize > 0 { // If bufferSize is 0, this check is different
		t.Errorf("Failed to receive initial messages after timeout test. Expected %d.", bufferSize)
	}
	if ok && len(msgsReceived) != bufferSize {
		t.Errorf("Expected %d messages from setup, got %d. The timed-out message might have been delivered.", bufferSize, len(msgsReceived))
	}
	for _, msg := range msgsReceived {
		if msg.Data == msgToTimeout {
			t.Errorf("The message '%s' which should have timed out was received by the subscriber.", msgToTimeout)
		}
	}

	t.Log("TopicConfig (PublishTimeout > 0) tested successfully.")
}

// TestDefaultTopicBehavior tests that topics not explicitly created use DefaultPublishTimeout.
func TestDefaultTopicBehavior(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	topic := "default_topic_behavior"
	bufferSize := 1
	// No ps.CreateTopic(), so it should use defaults (AllowDropping: false, PublishTimeout: DefaultPublishTimeout)

	sub := setupFullInternalChannel(t, ps, topic, "default_sub_behavior", bufferSize)

	msgToTimeout := "Msg that Should Timeout (default)"
	publishDone := make(chan bool, 1)

	go func() {
		startTime := time.Now()
		ps.Publish(Message{Topic: topic, Data: msgToTimeout})
		elapsed := time.Since(startTime)
		// Check if elapsed is around DefaultPublishTimeout.
		if elapsed >= DefaultPublishTimeout-(50*time.Millisecond) { // Allow some leeway
			publishDone <- true
		} else {
			publishDone <- false
		}
	}()

	didItTimeout := false
	select {
	case result := <-publishDone:
		didItTimeout = result
		if result {
			t.Logf("Publish call for default topic completed and took expected duration for timeout (%s).", DefaultPublishTimeout)
		} else {
			t.Errorf("Publish call for default topic completed too quickly, suggesting it did not time out as expected with DefaultPublishTimeout.")
		}
	case <-time.After(DefaultPublishTimeout + 200*time.Millisecond): // Increased wait
		t.Fatal("Publish goroutine for default topic did not complete in expected time.")
	}

	if !didItTimeout {
		t.Error("Expected publish operation on default topic to timeout, but it seems it did not.")
	}

	// Verify only messages from setup are received.
	msgsReceived, ok := waitForMessages(t, sub.Ch, bufferSize, time.Duration(bufferSize+1)*100*time.Millisecond)
	if !ok && bufferSize > 0 {
		t.Errorf("Failed to receive initial messages after default timeout test. Expected %d.", bufferSize)
	}
	if ok && len(msgsReceived) != bufferSize {
		t.Errorf("Expected %d messages from setup for default topic, got %d.", bufferSize, len(msgsReceived))
	}
	for _, msg := range msgsReceived {
		if msg.Data == msgToTimeout {
			t.Errorf("The message '%s' which should have timed out on default topic was received.", msgToTimeout)
		}
	}

	t.Log("Default topic behavior (DefaultPublishTimeout) tested successfully.")
}

// TestCleanupSubGoroutineDrain ensures CleanupSub initiates channel drain in a goroutine.
func TestCleanupSubGoroutineDrain(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	topic := "cleanup_drain_topic"
	sub := ps.Subscribe(topic, "drain_sub", 5) // Use a buffer
	if sub == nil {
		t.Fatal("Subscription returned nil")
	}

	ps.Publish(Message{Topic: topic, Data: "Msg 1"})
	ps.Publish(Message{Topic: topic, Data: "Msg 2"})
	// These messages go to internalCh. deliverMessages will pick them up.
	// If sub.Ch isn't read, deliverMessages will block on the first one.

	ps.CleanupSub(sub) // This should signal deliverMessages to stop & close sub.Ch eventually.

	ps.Publish(Message{Topic: topic, Data: "Msg 3 (after cleanup)"}) // Should not be seen by 'sub'

	// Check if sub.Ch is closed or yields no more messages.
	select {
	case msg, chanOk := <-sub.Ch:
		if chanOk {
			// This is possible if messages were in flight and drained by the cleanup drainer *before*
			// deliverMessages fully closed sub.Ch, or if deliverMessages sent something before closing.
			t.Logf("Subscriber channel yielded a message '%v' during/after CleanupSub. This might be a buffered/in-flight message.", msg.Data)
			// Check if further messages exist or if it closes
			select {
			case nextMsg, nextOk := <-sub.Ch:
				if nextOk {
					t.Errorf("Subscriber channel yielded another message '%v' after first read post-CleanupSub.", nextMsg.Data)
				} else {
					t.Log("Subscriber channel closed after yielding one in-flight message, as expected.")
				}
			case <-time.After(100 * time.Millisecond):
				t.Log("Subscriber channel yielded one message then no more (timed out).")
			}
		} else {
			t.Log("Subscriber channel closed by CleanupSub, as expected.")
		}
	case <-time.After(200 * time.Millisecond): // Increased timeout
		t.Log("Subscriber channel received no new messages after cleanup (timed out). This is okay if already drained/empty.")
	}

	ps.mu.RLock()
	defer ps.mu.RUnlock()
	if topicSubs, topicExists := ps.subscribers[topic]; topicExists {
		if _, subExists := topicSubs[sub.ID]; subExists {
			t.Errorf("Subscriber '%s' still found in pubsub map after CleanupSub.", sub.ID)
		}
	}
	t.Log("CleanupSub goroutine drain tested successfully.")
}

// TestClose ensures the PubSub system shuts down gracefully.
func TestClose(t *testing.T) {
	ps := NewPubSub()

	topic1 := "close_topic_1"
	topic2 := "close_topic_2"

	sub1 := ps.Subscribe(topic1, "sub1", 5)
	sub2 := ps.Subscribe(topic1, "sub2", 5)
	sub3 := ps.Subscribe(topic2, "sub3", 5)

	if sub1 == nil || sub2 == nil || sub3 == nil {
		t.Fatal("Subscription returned nil")
	}

	var readerWg sync.WaitGroup
	startReader := func(s *Subscriber, id string) {
		readerWg.Add(1)
		go func() {
			defer readerWg.Done()
			for range s.Ch {
				// Consume
			}
			t.Logf("Reader for %s exited.", id)
		}()
	}
	startReader(sub1, "sub1")
	startReader(sub2, "sub2")
	startReader(sub3, "sub3")

	ps.Publish(Message{Topic: topic1, Data: "Closing Msg 1"})
	ps.Publish(Message{Topic: topic2, Data: "Closing Msg 2"})

	t.Log("Calling PubSub.Close()...")
	ps.Close()
	t.Log("PubSub.Close() returned.")

	doneWaitingForReaders := make(chan struct{})
	go func() {
		readerWg.Wait()
		close(doneWaitingForReaders)
	}()

	select {
	case <-doneWaitingForReaders:
		t.Log("All subscriber client reader goroutines exited gracefully after PubSub.Close().")
	case <-time.After(2 * time.Second): // Increased timeout
		t.Error("Subscriber client reader goroutines did not all exit after PubSub.Close(). Possible channel not closed or leak.")
	}

	ps.Publish(Message{Topic: topic1, Data: "After Close"}) // Should not panic, effectively a no-op

	ps.mu.RLock()
	defer ps.mu.RUnlock()
	if len(ps.subscribers) != 0 {
		t.Errorf("Subscribers map not empty after Close. Contains: %+v", ps.subscribers)
	}
	if len(ps.topicConfigs) != 0 {
		t.Errorf("Topic configs map not empty after Close. Contains: %+v", ps.topicConfigs)
	}
	t.Log("PubSub.Close() tested successfully.")
}

// TestConcurrentSubscribeUnsubscribe ensures concurrency safety.
func TestConcurrentSubscribeUnsubscribe(t *testing.T) {
	// This test can be flaky due to its high concurrency and short sleeps.
	// It's primarily a race condition detector.
	// Consider running with -race flag.
	t.Parallel() // Allow this test to run in parallel with others

	ps := NewPubSub()
	defer ps.Close()

	topic := "concurrent_topic_stress"
	numGoroutines := 5           // Number of concurrent actors
	iterationsPerGoroutine := 10 // Number of subscribe/publish/cleanup cycles per actor

	var stressWg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		stressWg.Add(1)
		go func(goroutineID int) {
			defer stressWg.Done()
			subIDBase := fmt.Sprintf("g%d-sub", goroutineID)
			for j := 0; j < iterationsPerGoroutine; j++ {
				currentSubID := fmt.Sprintf("%s-iter%d", subIDBase, j)
				sub := ps.Subscribe(topic, currentSubID, 2) // Small buffer
				if sub == nil {
					// This might happen if another goroutine just subscribed with the exact same ID in a tiny window.
					// Or if Subscribe has an issue. For this test, log it and continue.
					t.Logf("Goroutine %d, Iter %d: Subscribe for %s returned nil", goroutineID, j, currentSubID)
					continue
				}

				// Do some work with the subscriber
				ps.Publish(Message{Topic: topic, Data: fmt.Sprintf("Msg from %s", currentSubID)})

				// Attempt to read, but don't block test if message is missed due to timing.
				// The main purpose is to stress subscribe/cleanup.
				msgReceived := false
				select {
				case _, ok := <-sub.Ch:
					if ok {
						msgReceived = true
					}
				case <-time.After(20 * time.Millisecond): // Short timeout
					// Message might be missed, or sub might be cleaned up quickly
				}
				_ = msgReceived // Use variable to avoid lint error

				ps.CleanupSub(sub)
				// time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond) // Optional small random delay
			}
		}(i)
	}
	stressWg.Wait()
	t.Log("Concurrent subscribe/unsubscribe stress test completed.")
}

// TestConcurrentPublish ensures no deadlocks or races during high publish volume.
func TestConcurrentPublish(t *testing.T) {
	t.Parallel() // Allow this test to run in parallel with others

	ps := NewPubSub()
	defer ps.Close()

	topic := "concurrent_publish_stress"
	ps.CreateTopic(topic, TopicConfig{AllowDropping: true}) // Use dropping for max concurrency

	numSubscribers := 5
	numPublishers := 5
	messagesPerPublisher := 50

	var subscriberDrainWg sync.WaitGroup
	for i := 0; i < numSubscribers; i++ {
		sub := ps.Subscribe(topic, fmt.Sprintf("stress-sub-%d", i), 5) // Small buffer
		if sub == nil {
			t.Fatalf("Failed to subscribe stress-sub-%d", i)
		}
		subscriberDrainWg.Add(1)
		go func(s *Subscriber) {
			defer subscriberDrainWg.Done()
			for range s.Ch { // Drain messages
			}
		}(sub)
	}

	var publisherWg sync.WaitGroup
	for i := 0; i < numPublishers; i++ {
		publisherWg.Add(1)
		go func(publisherID int) {
			defer publisherWg.Done()
			for j := 0; j < messagesPerPublisher; j++ {
				ps.Publish(Message{Topic: topic, Data: fmt.Sprintf("Pub %d - Msg %d", publisherID, j)})
			}
		}(i)
	}

	publisherWg.Wait() // Wait for all publishers to finish
	// After publishers are done, Close will initiate cleanup of subscribers.
	// We need to wait for subscribers to finish draining what they've received *before* Close fully blocks.
	// However, Close() itself handles waiting for subscribers.
	// The main check here is that publishing completes.
	// The defer ps.Close() will then handle shutdown and its own waits.

	t.Log("Concurrent publish stress test completed (publishers finished). Defer Close will handle subscriber shutdown.")
}

func TestReadMessages(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	// Create a subscriber that listens on "test_topic"
	subID := "test_subscriber"
	sub := ps.Subscribe("test_topic", subID, 1)
	if sub == nil {
		t.Fatalf("Failed to subscribe sub")
	}
	defer ps.CleanupSub(sub) // Ensure sub is cleaned up

	// Create a channel to signal when the message is received
	msgReceived := make(chan struct{})

	// Start ReadMessages in a goroutine and use a channel to signal when it's done
	var receivedMessages []Message
	go func() {
		sub.ReadMessages(func(msg Message) {
			receivedMessages = append(receivedMessages, msg)
			close(msgReceived) // Signal that the message has been received
		})
	}()

	// Publish a message to the topic
	ps.Publish(Message{Topic: "test_topic", Data: "Test Message"})

	// Wait for the message to be processed
	<-msgReceived

	if len(receivedMessages) != 1 {
		t.Errorf("Expected 1 message, got %d", len(receivedMessages))
	}
	if receivedMessages[0].Data != "Test Message" {
		t.Errorf("Expected message data 'Test Message', got '%v'", receivedMessages[0].Data)
	}
}

// -----------------------------------------------------------------------------
// Tests for Cleanable Interface
// -----------------------------------------------------------------------------

// cleanableResource is a helper struct for testing the Cleanable interface.
type cleanableResource struct {
	ID        int
	cleanedUp atomic.Bool
}

// Cleanup implements the Cleanable interface.
func (cr *cleanableResource) Cleanup() {
	cr.cleanedUp.Store(true)
	logDebug("Cleanup called for resource ID: %d", cr.ID)
}

// WasCleaned checks if the Cleanup method was called.
func (cr *cleanableResource) WasCleaned() bool {
	return cr.cleanedUp.Load()
}

// TestCleanable_MessageDropped_FullBuffer tests that Cleanup is called when a message is dropped due to a full buffer.
func TestCleanable_MessageDropped_FullBuffer(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	topic := "cleanable-drop-topic"
	ps.CreateTopic(topic, TopicConfig{AllowDropping: true})

	sub := ps.Subscribe(topic, "resource-worker", 1)
	if sub == nil {
		t.Fatal("Failed to subscribe")
	}

	msg1Data := &cleanableResource{ID: 1}
	ps.Publish(Message{Topic: topic, Data: msg1Data})

	msg2Data := &cleanableResource{ID: 2}
	ps.Publish(Message{Topic: topic, Data: msg2Data})

	time.Sleep(100 * time.Millisecond)

	if msg1Data.WasCleaned() {
		t.Error("Message 1 should not have been cleaned up, but it was.")
	}

	if !msg2Data.WasCleaned() {
		t.Error("Message 2 should have been cleaned up because it was dropped, but it wasn't.")
	} else {
		t.Log("Successfully cleaned up message dropped due to full buffer.")
	}
}

// TestCleanable_MessageDropped_Timeout tests that Cleanup is called when a publish times out.
func TestCleanable_MessageDropped_Timeout(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	topic := "cleanable-timeout-topic"
	timeoutDuration := 100 * time.Millisecond
	ps.CreateTopic(topic, TopicConfig{AllowDropping: false, PublishTimeout: timeoutDuration})

	sub := ps.Subscribe(topic, "resource-worker", 1)
	if sub == nil {
		t.Fatal("Failed to subscribe")
	}

	ps.Publish(Message{Topic: topic, Data: &cleanableResource{ID: 1}})
	time.Sleep(20 * time.Millisecond)
	msg2Data := &cleanableResource{ID: 2}
	ps.Publish(Message{Topic: topic, Data: msg2Data})
	msg3Data := &cleanableResource{ID: 3}
	ps.Publish(Message{Topic: topic, Data: msg3Data})

	time.Sleep(50 * time.Millisecond)

	if msg2Data.WasCleaned() {
		t.Error("Message 2 should not have been cleaned up, but it was.")
	}

	if !msg3Data.WasCleaned() {
		t.Error("Message 3 should have been cleaned up due to publish timeout, but it wasn't.")
	} else {
		t.Log("Successfully cleaned up message dropped due to publish timeout.")
	}
}

// TestCleanable_MessageDropped_NoSubscribers tests that Cleanup is called when there are no subscribers.
func TestCleanable_MessageDropped_NoSubscribers(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	topic := "cleanable-no-subs-topic"
	msgData := &cleanableResource{ID: 1}

	ps.Publish(Message{Topic: topic, Data: msgData})
	time.Sleep(50 * time.Millisecond)

	if !msgData.WasCleaned() {
		t.Error("Message should have been cleaned up when published to a topic with no subscribers, but it wasn't.")
	} else {
		t.Log("Successfully cleaned up message published to a topic with no subscribers.")
	}
}

// TestCleanable_MessageCleanedOnSubscriberCleanup tests that Cleanup is called for buffered messages when a subscriber is removed.
func TestCleanable_MessageCleanedOnSubscriberCleanup(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	topic := "cleanable-sub-cleanup-topic"
	sub := ps.Subscribe(topic, "worker", 5)
	if sub == nil {
		t.Fatal("Failed to subscribe")
	}

	msg1Data := &cleanableResource{ID: 1}
	msg2Data := &cleanableResource{ID: 2}

	ps.Publish(Message{Topic: topic, Data: msg1Data})
	ps.Publish(Message{Topic: topic, Data: msg2Data})
	time.Sleep(50 * time.Millisecond)

	sub.Unsubscribe()

	if !msg1Data.WasCleaned() {
		t.Errorf("Message 1 should have been cleaned up during subscriber cleanup, but it wasn't.")
	}
	if !msg2Data.WasCleaned() {
		t.Errorf("Message 2 should have been cleaned up during subscriber cleanup, but it wasn't.")
	}
	t.Log("Successfully cleaned up buffered messages during subscriber cleanup.")
}

// TestCleanable_MessageNotCleanedWhenDelivered tests that Cleanup is NOT called for a successfully delivered message.
func TestCleanable_MessageNotCleanedWhenDelivered(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	topic := "cleanable-success-topic"
	sub := ps.Subscribe(topic, "worker", 5)
	if sub == nil {
		t.Fatal("Failed to subscribe")
	}

	msgData := &cleanableResource{ID: 1}
	done := make(chan struct{})

	go func() {
		<-sub.Ch // Read one message.
		close(done)
	}()

	ps.Publish(Message{Topic: topic, Data: msgData})

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for message to be consumed.")
	}

	if msgData.WasCleaned() {
		t.Error("Message was cleaned up even though it was successfully delivered.")
	} else {
		t.Log("Successfully verified that delivered message was not cleaned up.")
	}
}
