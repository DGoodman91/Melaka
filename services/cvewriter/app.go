package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/segmentio/kafka-go"
)

var kafkaReader *kafka.Reader

func init() {
	kafkaServer := readFromENV("KAFKA_BROKER", "localhost:9092")
	kafkaTopic := readFromENV("KAFKA_TOPIC", "nvd-cves")

	kafkaReader = newKafkaReader(kafkaServer, kafkaTopic)

	fmt.Println("Kafka Broker - ", kafkaServer)
	fmt.Println("Kafka Topic - ", kafkaTopic)

}

func main() {
	for {
		m, err := kafkaReader.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

	if err := kafkaReader.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}

func newKafkaReader(kafkaURL, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{kafkaURL},
		Topic:     topic,
		Partition: 0,
		MaxBytes:  10e6,
	})
}

func readFromENV(key, defaultVal string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultVal
	}
	return value
}
