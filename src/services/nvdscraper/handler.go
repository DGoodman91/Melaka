package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
)

type CveHandler interface {
	WriteCves(cves []CveMsg) error
	Close() error
}

type KafkaHandler struct {
	Writer *kafka.Writer
}

func (k *KafkaHandler) WriteCves(cves []CveMsg) error {

	l := len(cves)
	msgs := make([]kafka.Message, l)

	for i, c := range cves {
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

		msgs[i] = msg

	}

	go k.enqueueMessages(msgs)

	return nil
}

func (k *KafkaHandler) enqueueMessages(msgs []kafka.Message) {

	// Write the message to Kafka
	if err := k.Writer.WriteMessages(context.Background(), msgs...); err != nil {
		log.Printf("Failed to enqueue messages, error: %s\n", err)
	} else {
		for _, msg := range msgs {
			log.Printf("Enqueued data for %s\n", msg.Key)
		}
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
