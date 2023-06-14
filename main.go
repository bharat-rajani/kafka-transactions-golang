package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

// TransactionalService is just a simple object which covers
type TransactionalService struct {
	sinkProducer   *kafka.Producer
	sourceConsumer *kafka.Consumer
	sourceTopic    string
	sinkTopic      string
}

func (t *TransactionalService) Run(ctx context.Context) error {

	// 1. Initialize the transaction
	err := t.sinkProducer.InitTransactions(ctx)
	if err != nil {
		return fmt.Errorf("Error initializing transaction%e\n", err)
	}

	// 2. Consume message from source topic
	err = t.sourceConsumer.Subscribe(t.sourceTopic, nil)
	if err != nil {
		return fmt.Errorf("error while subscribing to source topic %e\n", err)
	}

	// A long-running loop which will consume the message
	// 1 consume the message
	//	 1.a Initiate the transaction
	// 	 1.b transform (processMessageInTransaction)
	// 	 1.c produce (processMessageInTransaction)
	// 	 1.d send consumer offsets to producer for committing (processMessageInTransaction)
	// 	 1.e commit the transaction (processMessageInTransaction)

	run := true
	for run == true {
		select {
		case <-ctx.Done():
			log.Println("context done received, exiting")
			if err = t.sourceConsumer.Close(); err != nil {
				log.Printf("%e\n", err)
			}
			run = false
		default:
			ev := t.sourceConsumer.Poll(100)
			switch e := ev.(type) {
			case *kafka.Message:
				err := t.processMessageInTransaction(ctx, ev.(*kafka.Message))
				if err != nil {
					run = false
				}
				run = true
			case kafka.PartitionEOF:
				log.Printf("%% Reached %v\n", e)
				run = true
			case kafka.Error:
				log.Printf("kafka error event: %v", e.Error())
				run = true
			default:
				//log.Printf("Polling, received nil event %v\n", e)
				run = true
			}
		}
	}
	return nil
}

func (t *TransactionalService) processMessageInTransaction(ctx context.Context, kafkaMsg *kafka.Message) error {

	// 3. Begin the transaction
	err := t.sinkProducer.BeginTransaction()
	if err != nil {
		return fmt.Errorf("error beginning transaction %e", err)
	}
	log.Printf("Message received, partition: %d, offset: %s, msg:%v\n", kafkaMsg.TopicPartition.Partition, kafkaMsg.TopicPartition.Offset.String(), string(kafkaMsg.Value))
	//panic("cosmetic panic, message should be consumed again")

	var m map[string]interface{}
	err = json.Unmarshal(kafkaMsg.Value, &m)
	if err != nil {
		return err
	}

	// increment the number
	m["number"] = m["number"].(float64) + 1
	messageBytes, err := json.Marshal(m)
	deliveryChan := make(chan kafka.Event)

	// 4. Produce message to sink topic
	err = t.sinkProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &t.sinkTopic, Partition: kafka.PartitionAny},
		Value:          messageBytes,
		Headers:        nil,
	}, deliveryChan)
	if err != nil {
		if err := t.sinkProducer.AbortTransaction(ctx); err != nil {
			return err
		}
	}

	// wait for ack from brokers for this message
	e := <-deliveryChan
	messageAck := e.(*kafka.Message)
	if messageAck.TopicPartition.Error != nil {
		log.Printf("Delivery failed: %v\n", messageAck.TopicPartition.Error)
		return err
	} else {
		log.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*messageAck.TopicPartition.Topic, messageAck.TopicPartition.Partition, messageAck.TopicPartition.Offset)
	}

	metadata, err := t.sourceConsumer.GetConsumerGroupMetadata()
	assignment, err := t.sourceConsumer.Assignment()
	//log.Printf("%e\n", err)
	position, err := t.sourceConsumer.Position(assignment)
	//log.Printf("%e\n", err)

	// 5. Send offset to transaction coordinator
	err = t.sinkProducer.SendOffsetsToTransaction(ctx, position, metadata)
	//log.Printf("%e\n", err)

CommitTransaction:
	// 6. Commit transaction (re-triable)
	err = t.sinkProducer.CommitTransaction(ctx)
	if err == nil {
		return nil
	} else if err.(kafka.Error).TxnRequiresAbort() {
		err := t.sinkProducer.AbortTransaction(ctx)
		if err != nil {
			return err
		}
	} else if err.(kafka.Error).IsRetriable() {
		goto CommitTransaction
	} else { // treat all other errors as fatal errors
		panic(err)
	}
	if err != nil {
		return err
	}

	return nil
}

func main() {

	ctx := context.Background()
	sourceTopic, sinkTopic := "addition", "results"
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":      "localhost",
		"go.logs.channel.enable": true,
		"transactional.id":       fmt.Sprintf("%s.%d", sourceTopic, 1),
	})
	if err != nil {
		log.Println(err)
		return
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":      "localhost",
		"group.id":               "orchestrator",
		"auto.offset.reset":      "earliest",
		"enable.auto.commit":     false,
		"isolation.level":        "read_committed",
		"go.logs.channel.enable": true,
	})
	if err != nil {
		log.Println(err)
		return
	}

	go LogKafka(producer.Logs())
	go LogKafka(consumer.Logs())

	svc := TransactionalService{
		sinkProducer:   producer,
		sourceConsumer: consumer,
		sourceTopic:    sourceTopic,
		sinkTopic:      sinkTopic,
	}
	err = svc.Run(ctx)
	if err != nil {
		log.Println(err)
		return
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
