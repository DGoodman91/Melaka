package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/segmentio/kafka-go"
)

type CveHandler interface {
	WriteCves(cves []CveMsg, wg *sync.WaitGroup) error
	Close() error
}

type KafkaHandler struct {
	Writer *kafka.Writer
}

func (k *KafkaHandler) WriteCve(c CveMsg, wg *sync.WaitGroup) error {

	value, err := json.Marshal(c)
	if err != nil {
		log.Printf("Failed to serialize CVE data into JSON: %s", err)
		return err
	}

	key := c.Cve.ID
	msg := kafka.Message{
		Key:   []byte(key),
		Value: []byte(value),
	}

	// Increment the wait group before starting the goroutine
	wg.Add(1)
	go k.enqueueMessage(msg, wg)

	return nil
}

func (k *KafkaHandler) enqueueMessage(msg kafka.Message, wg *sync.WaitGroup) {

	defer wg.Done() // Decrement the WaitGroup when the goroutine completes

	// Write the message to Kafka
	if err := k.Writer.WriteMessages(context.Background(), msg); err != nil {
		log.Printf("Failed to enqueue message %s: %s\n", msg.Value, err)
	} else {
		log.Printf("Enqueued data for %s\n", msg.Key)
	}

}

func (k *KafkaHandler) Close() error {
	return nil
}

func newKafkaHandler(kafkaServer string, kafkaTopic string) *KafkaHandler {
	kafkaWriter := newKafkaWriter(kafkaServer, kafkaTopic)
	return &KafkaHandler{
		Writer: kafkaWriter,
	}
}

func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL), // TODO handling for multiple brokers
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}
