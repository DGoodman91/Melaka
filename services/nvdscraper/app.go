package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

var kafkaServer, kafkaTopic string

func init() {
	kafkaServer = readFromENV("KAFKA_BROKER", "localhost:9092")
	kafkaTopic = readFromENV("KAFKA_TOPIC", "nvd-cves")

	fmt.Println("Kafka Broker - ", kafkaServer)
	fmt.Println("Kafka topic - ", kafkaTopic)
}

func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func main() {

	time.Sleep(10 * time.Second) // temp hack to wait for kafka to start accepting before we have proper connection/retry handling

	resp, getErr := http.Get("https://services.nvd.nist.gov/rest/json/cves/2.0?cveId=CVE-2019-1010218")
	if getErr != nil {
		log.Fatalf("HTTP request failed: %s", getErr) // Fatalln prints then exits
	}

	body, ioReadErr := io.ReadAll(resp.Body)
	if ioReadErr != nil {
		log.Fatalf("Failed to read body from HTTP response: %s", ioReadErr)
	}

	var result Response
	if unmarshalErr := json.Unmarshal(body, &result); unmarshalErr != nil {
		log.Fatalf("Failed to parse response body into JSON: %s", unmarshalErr)
	}

	writer := newKafkaWriter(kafkaServer, kafkaTopic)
	defer writer.Close()

	value, marshalErr := json.Marshal(result.Vulnerabilities[0])
	if marshalErr != nil {
		log.Fatalf("Failed to serialize CVE data into JSON: %s", marshalErr)
	}

	key := result.Vulnerabilities[0].Cve.ID
	msg := kafka.Message{
		Key:   []byte(kafkaTopic),
		Value: []byte(value),
	}
	writerError := writer.WriteMessages(context.Background(), msg)
	if writerError != nil {
		fmt.Printf("Failed to enqueue message %s: %s", value, writerError)
	} else {
		fmt.Println("produced", key)
	}

}

func readFromENV(key, defaultVal string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultVal
	}
	return value
}
