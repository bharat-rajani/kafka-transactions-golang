package main

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"os"
	"sync"
	"sync/atomic"
)

func main() {
	ctx := context.Background()
	resultTopic := "results-1"
	cfg := &kafka.ConfigMap{
		"bootstrap.servers":      "localhost",
		"group.id":               "result.1",
		"auto.offset.reset":      "earliest",
		"isolation.level":        "read_committed",
		"go.logs.channel.enable": true,
	}

	c, err := kafka.NewConsumer(cfg)
	if err != nil {
		log.Printf("error while creating kafka event consumer in orchestrator: %e", err)
		return
	}
	//go LogKafka(c.Logs())

	var wg sync.WaitGroup
	wg.Add(1)
	go Consume(ctx, resultTopic, c, &wg)
	wg.Wait()
}

func Consume(ctx context.Context, topic string, c *kafka.Consumer, wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
	}()
	err := c.Subscribe(topic, nil)
	if err != nil {
		log.Printf("error while subscribing to source topic: %e\n", err)
		return
	}
	var ops uint64
	run := true
	for run == true {
		select {
		case <-ctx.Done():
			log.Println("context done received, exiting ")
			if err = c.Close(); err != nil {
				log.Println(err)
			}
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				// Process the message received.
				fmt.Printf("Message on %s:%s\n", e.TopicPartition, string(e.Value))
				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}
				atomic.AddUint64(&ops, 1)

				// ensuring at-least once processing.
				_, err := c.StoreMessage(e)
				if err != nil {
					//fmt.Fprintf(os.Stderr, "Error storing offset after message %s %e:\n",
					//	e.TopicPartition, err)
				}
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "Error: %v: %v\n", e.Code(), e)
			default:
				//fmt.Printf("Ignored %v\n", e)
				fmt.Printf("Processed %d\n", ops)
			}
		}
	}
}

func LogKafka(logChan chan kafka.LogEvent) {

	for {
		select {
		case logEvent, ok := <-logChan:
			if !ok {
				return
			}
			log.Println(logEvent.String())
		}
	}
}
