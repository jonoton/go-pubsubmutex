/*
Package pubsubmutex implements a thread-safe, in-memory, multi-topic publish-subscribe
system that provides type safety for each topic.

This package allows you to create a single pub/sub instance that can handle different
data types on different topics. It ensures that you can only publish or subscribe
to a topic with the data type it was registered with.

# Key Features

  - Type-Safe Topics: Register each topic with a specific Go type. The generic
    `Publish` and `Subscribe` functions then check this type at runtime, returning
    an error if there is a mismatch.

  - Centralized Management: Use a single `PubSub` instance to manage multiple,
    type-safe topic systems.

  - Thread Safety: All operations are safe for concurrent use by multiple goroutines.

  - Reference Counting & Cleanup: Message data can implement the `ManagedItem[T]`
    interface. The system will automatically call `Ref()` when creating new references
    for subscribers and `Cleanup()` when a message reference is dropped, preventing
    resource leaks.

  - Configurable Delivery: Configure topics to either drop messages or block with a
    timeout if a subscriber's buffer is full. This can be done at registration
    time or later via `UpdateTopic`.

  - Subscriber Self-Cleanup: Subscriber instances can unsubscribe themselves via
    the `Unsubscribe()` method.

# Usage Examples

Here are some examples demonstrating how to use the type-safe API.

# Initialization and Topic Registration

First, create a `PubSub` instance. Before you can use a topic, you must register it
with the specific data type it will carry. You can optionally provide a configuration
at the same time.

	// Create a new PubSub system.
	ps := pubsubmutex.NewPubSub()
	defer ps.Close() // Best practice to defer Close().

	// Define the types for your topics.
	type UserUpdate struct{ UserID int; NewEmail string }
	type OrderEvent struct{ OrderID string; Status string }

	// Register a topic with default settings.
	pubsubmutex.RegisterTopic[UserUpdate](ps, "user.updates")

	// Register another topic and configure it to drop messages if buffers are full.
	pubsubmutex.RegisterTopic[OrderEvent](ps, "order.events", pubsubmutex.TopicConfig{AllowDropping: true})

# Type-Safe Subscribing and Publishing

Once a topic is registered, you can use the generic `Subscribe` and `Publish`
functions. They will return an error at runtime if you use the wrong type.

	// Assumes 'ps' is created and topics are registered from the previous example.
	var wg sync.WaitGroup

	// Subscribe to the "user.updates" topic, getting a type-safe subscriber.
	userSub, err := pubsubmutex.Subscribe[UserUpdate](ps, "user.updates", "user-service", 10)
	if err != nil {
		// Handle error
	}

	// This call will fail at runtime and return an error because the topic
	// "user.updates" is registered for UserUpdate, not OrderEvent.
	orderSub, err := pubsubmutex.Subscribe[OrderEvent](ps, "user.updates", "order-service", 10)
	if err != nil {
		fmt.Println("Correctly caught type mismatch error:", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range userSub.Ch { // msg is of type pubsubmutex.Message[UserUpdate]
			fmt.Printf("User update received: ID=%d, NewEmail=%s\n", msg.Data.UserID, msg.Data.NewEmail)
		}
	}()

	// Publishing is also checked at runtime.
	updateMsg := pubsubmutex.Message[UserUpdate]{
		Topic: "user.updates",
		Data:  UserUpdate{UserID: 123, NewEmail: "new.email@example.com"},
	}
	err = pubsubmutex.Publish(ps, updateMsg)
	if err != nil {
		// Handle error
	}

	// This publish will fail at runtime and return an error.
	errorMsg := pubsubmutex.Message[OrderEvent]{
		Topic: "user.updates",
		Data:  OrderEvent{OrderID: "xyz"},
	}
	err = pubsubmutex.Publish(ps, errorMsg)
	if err != nil {
		fmt.Println("Correctly caught publish type mismatch error:", err)
	}

# Using ManagedItem for Resource Cleanup

If your message data holds a resource (like a file handle or pointer), you can
implement the `ManagedItem[T]` interface to manage its lifecycle.

	// Define a type that holds a resource and a reference count.
	type ResourcefulMessage struct {
		ID       int
		refCount int32
	}

	// Implement the ManagedItem interface.
	func (rm *ResourcefulMessage) Ref() *ResourcefulMessage {
		atomic.AddInt32(&rm.refCount, 1)
		return rm
	}

	func (rm *ResourcefulMessage) Cleanup() {
		if atomic.AddInt32(&rm.refCount, -1) == 0 {
			fmt.Printf("Final cleanup for resource ID: %d\n", rm.ID)
			// Here you would close the file handle, etc.
		}
	}

	// ... later in your code ...
	topic := "resource.topic"
	pubsubmutex.RegisterTopic[*ResourcefulMessage](ps, topic)

	// Publish to a topic with no subscribers. The message will be dropped,
	// and its Cleanup() method will be called automatically.
	resourceMsg := pubsubmutex.Message[*ResourcefulMessage]{
		Topic: topic,
		Data:  &ResourcefulMessage{ID: 1, refCount: 1},
	}
	pubsubmutex.Publish(ps, resourceMsg)
	// Output will include: Final cleanup for resource ID: 1
*/
package pubsubmutex
