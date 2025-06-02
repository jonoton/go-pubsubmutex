package main

import (
	"fmt"
	"time"

	pubsub "github.com/jonoton/go-pubsubmutex"
)

func main() {
	pubsub.SetDebug(true) // Enable debug logging

	ps := pubsub.NewPubSub()
	defer ps.Close()

	// Configure topics
	// "news" will default to no dropping (publisher blocks if buffer full)
	// "sports" will be explicitly set to allow dropping
	ps.CreateTopic("sports", pubsub.TopicConfig{AllowDropping: true})
	// No CreateTopic call for "news" means it defaults to AllowDropping: false

	// Define subscriber message handlers
	// Sub1 is slow, likely to cause backpressure or drops depending on topic config
	subscriber1Handler := func(msg pubsub.Message) {
		fmt.Printf("(%s) received message: Topic='%s', Data='%v'\n", "sub1", msg.Topic, msg.Data)
		if msg.Topic == "sports" || (msg.Topic == "news" && msg.Data == "Breaking News: GoLang is awesome!") {
			time.Sleep(300 * time.Millisecond) // Simulate slow processing
		}
	}

	subscriber2Handler := func(msg pubsub.Message) {
		fmt.Printf("(%s) received message: Topic='%s', Data='%v'\n", "sub2", msg.Topic, msg.Data)
	}

	subscriber3Handler := func(msg pubsub.Message) {
		fmt.Printf("(%s) received message: Topic='%s', Data='%v'\n", "sub3", msg.Topic, msg.Data)
	}

	// Subscribe subscribers
	sub1 := ps.Subscribe("news", "sub1", 2)   // Small buffer for sub1, testing blocking
	sub2 := ps.Subscribe("sports", "sub2", 1) // Very small buffer for sub2, testing dropping
	sub3 := ps.Subscribe("news", "sub3", 5)

	go sub1.ReadMessages(subscriber1Handler)
	go sub2.ReadMessages(subscriber2Handler)
	go sub3.ReadMessages(subscriber3Handler)

	time.Sleep(100 * time.Millisecond) // Give goroutines time to start

	fmt.Println("\n--- Publishing to 'news' (no dropping) ---")
	// These publishes will block if sub1 or sub3's internal buffers fill up
	ps.Publish(pubsub.Message{Topic: "news", Data: "Breaking News: GoLang is awesome!"})
	ps.Publish(pubsub.Message{Topic: "news", Data: "Market Update 1"})
	ps.Publish(pubsub.Message{Topic: "news", Data: "Market Update 2"})
	ps.Publish(pubsub.Message{Topic: "news", Data: "Market Update 3"})
	fmt.Println("Main: Finished publishing to 'news'.")

	fmt.Println("\n--- Publishing to 'sports' (dropping allowed) ---")
	// These publishes might drop messages if sub2's internal buffer is full
	ps.Publish(pubsub.Message{Topic: "sports", Data: "Game Score 1"})
	ps.Publish(pubsub.Message{Topic: "sports", Data: "Game Score 2"})
	ps.Publish(pubsub.Message{Topic: "sports", Data: "Game Score 3"})
	ps.Publish(pubsub.Message{Topic: "sports", Data: "Game Score 4"})
	ps.Publish(pubsub.Message{Topic: "sports", Data: "Game Score 5"})
	fmt.Println("Main: Finished publishing to 'sports'.")

	fmt.Println("\n--- Unsubscribing sub1 ---")
	ps.Unsubscribe("news", "sub1")

	fmt.Println("\n--- Publishing more to 'news' ---")
	ps.Publish(pubsub.Message{Topic: "news", Data: "News after unsubscribe"})

	time.Sleep(2 * time.Second) // Give time for processing

	fmt.Println("\n--- Main goroutine finishing ---")
}
