/*
Package pubsubmutex implements a thread-safe, in-memory, topic-based
publish-subscribe system. It is designed for concurrent applications where
different parts of the system need to communicate asynchronously without
being directly coupled.

# Key Features

  - **Thread Safety:** All operations on the PubSub system, such as subscribing,
    publishing, and unsubscribing, are safe for concurrent use by multiple goroutines.

  - **Topic-Based Communication:** Clients subscribe to named topics and receive
    only the messages published to those specific topics.

  - **Configurable Message Delivery:** Topic behavior can be configured using
    TopicConfig. This allows control over whether messages should be dropped if a
    subscriber's buffer is full (AllowDropping) or if publishing should block
    with a specific timeout (PublishTimeout).

  - **Decoupled Architecture:** Each subscriber has an internal buffered channel that
    decouples the publisher from the consumer. A publisher can send a message
    without waiting for the subscriber to be ready to process it, improving system
    responsiveness.

  - **Subscriber Self-Cleanup:** Subscribers can manage their own lifecycle. A client
    holding a Subscriber instance can call its Unsubscribe() method to cleanly
    remove itself from the PubSub system.

  - **Automatic Resource Cleanup:** If a message's `Data` field implements the `Cleanable`
    interface (with a `Cleanup()` method), `Cleanup()` will be called automatically
    if the message is dropped. This occurs if a subscriber's buffer is full, a
    publish times out, the subscriber is closing, or if there are no subscribers
    for the topic at all. This prevents resource leaks.

  - **Graceful Shutdown:** The entire PubSub system can be shut down gracefully
    using the Close() method, which ensures all active subscribers are unsubscribed
    and their resources are released.

# Usage Examples

Here are some examples demonstrating how to use the package.

## Initialization and Subscribing

First, create a new PubSub system instance and subscribe to a topic. The `Subscribe`
method returns a `Subscriber` instance, which contains the channel you will use
to receive messages.

	// Create a new PubSub system.
	ps := pubsubmutex.NewPubSub()
	defer ps.Close() // Best practice to defer Close().

	// Subscribe to a topic with a unique subscriber ID and a buffer size of 10.
	sub1 := ps.Subscribe("news.sports", "subscriber-1", 10)
	if sub1 == nil {
		fmt.Println("Failed to subscribe")
		return
	}

	fmt.Printf("Successfully subscribed '%s' to topic '%s'.\n", sub1.ID, sub1.Topic)

## Publishing and Receiving Messages

Publish messages to a topic using `ps.Publish()`. To receive them, read from the
`Ch` channel on your `Subscriber` instance. It's common to do this in a separate goroutine.

	// Assumes 'ps' and 'sub1' exist from the previous example.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("Subscriber 1 waiting for messages...")
		for msg := range sub1.Ch {
			fmt.Printf("Subscriber 1 received: Topic='%s', Data='%v'\n", msg.Topic, msg.Data)
		}
		// The loop will exit when sub1.Ch is closed (e.g., by unsubscribing).
		fmt.Println("Subscriber 1 message channel closed.")
	}()

	// Publish messages to the topic.
	ps.Publish(pubsubmutex.Message{Topic: "news.sports", Data: "Welcome to sports news!"})
	ps.Publish(pubsubmutex.Message{Topic: "news.weather", Data: "This message will not be received by sub1."})
	ps.Publish(pubsubmutex.Message{Topic: "news.sports", Data: "A great match happened today."})

## Self-Unsubscribing

A subscriber can clean itself up by calling its `Unsubscribe()` method. This is often
done based on some condition, like receiving a specific message.

	ps := pubsubmutex.NewPubSub()
	defer ps.Close()

	var wg sync.WaitGroup
	sub := ps.Subscribe("commands", "worker-1", 5)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range sub.Ch {
			fmt.Printf("Worker received command: %v\n", msg.Data)
			if msg.Data == "stop" {
				fmt.Println("Stop command received. Unsubscribing...")
				sub.Unsubscribe() // Subscriber triggers its own cleanup.
			}
		}
		fmt.Println("Worker message loop exited.")
	}()

	ps.Publish(pubsubmutex.Message{Topic: "commands", Data: "start processing"})
	ps.Publish(pubsubmutex.Message{Topic: "commands", Data: "stop"})

	wg.Wait() // Wait for the worker goroutine to finish.

## Automatic Cleanup of Dropped Messages

If a message's payload needs to have resources freed (e.g., closing a file handle),
you can implement the `Cleanable` interface. Its `Cleanup()` method will be called
if the message is dropped.

	// Define a type that holds a resource.
	type ResourcefulMessage struct {
		ID       int
		resource string // In a real scenario, this might be a file handle, etc.
	}

	// Implement the Cleanable interface.
	func (rm *ResourcefulMessage) Cleanup() {
		fmt.Printf("Cleaning up resource for message ID: %d\n", rm.ID)
		// Here you would close the file handle, network connection, etc.
	}

	// ... later in the code ...

	// This topic is configured to drop messages immediately if the buffer is full.
	ps.CreateTopic("resource.topic", pubsubmutex.TopicConfig{AllowDropping: true})

	// Subscriber with a small buffer that is not reading messages, causing it to fill up.
	sub := ps.Subscribe("resource.topic", "resource-worker", 1)

	// Publish two messages. The first will fill the buffer. The second will be dropped.
	// The Cleanup() method for the second message will be called automatically.
	ps.Publish(pubsubmutex.Message{Topic: "resource.topic", Data: &ResourcefulMessage{ID: 1, resource: "active"}})
	ps.Publish(pubsubmutex.Message{Topic: "resource.topic", Data: &ResourcefulMessage{ID: 2, resource: "active"}})

	// Output will include:
	// Cleaning up resource for message ID: 2
*/
package pubsubmutex
