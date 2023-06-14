package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"log"
	"math/rand"
	"sync/atomic"
	"time"
)

const maxCount = 2

func main() {

	sourceTopic := "addition-1"
	ctx := context.Background()
	rand.Seed(time.Now().UnixNano())
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":      "localhost",
		"go.logs.channel.enable": true,
	})
	if err != nil {
		return
	}
	go LogKafka(p.Logs())

	done := make(chan struct{})
	go ProduceRandomTicker(ctx, sourceTopic, 5000*time.Millisecond, p, done)
	<-done
}

func ProduceRandomTicker(ctx context.Context, topic string, t time.Duration, p *kafka.Producer, done chan struct{}) {

	ticker := time.NewTicker(t)
	stp := make(chan struct{})
	deliveryChan := make(chan kafka.Event)
	go LogDeliveryEvents(deliveryChan)
	go func(stp chan struct{}) {
		defer func() {
			p.Close()
			stp <- struct{}{}
		}()
		for i := 0; i <= maxCount; {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				fmt.Println("Tick at", t)
				num := rand.Intn(10-1) + 1
				id := uuid.New()
				data := map[string]interface{}{
					"id":     id.String(),
					"number": num,
				}
				dataBytes, err := json.Marshal(data)
				if err != nil {
					log.Println(err)
					return
				}
				err = p.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
					Value:          dataBytes,
				}, deliveryChan)
				if err != nil {
					log.Println(err)
					return
				}
				i++
			}
		}
	}(stp)
	stop := <-stp
	ticker.Stop()
	fmt.Println("Producer stopped")
	done <- stop
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

func LogDeliveryEvents(logChan chan kafka.Event) {
	var ops uint64
	for {
		select {
		case e, ok := <-logChan:
			if !ok {
				return
			}
			switch e.(type) {
			case *kafka.Message:
				messageAck := e.(*kafka.Message)
				if messageAck.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v\n", messageAck.TopicPartition.Error)
				} else {
					log.Printf("Delivered message to topic %s [%d] at offset %v\n",
						*messageAck.TopicPartition.Topic, messageAck.TopicPartition.Partition, messageAck.TopicPartition.Offset)
					atomic.AddUint64(&ops, 1)
					log.Printf("Produced %d message\n", ops)
				}
			default:
				log.Printf("Delivery Event: %v\n", e)
			}

		}
	}
}
