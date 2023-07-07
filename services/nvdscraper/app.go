package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var kafkaServer, kafkaTopic string

func init() {
	kafkaServer = readFromENV("KAFKA_BROKER", "localhost:90912")
	kafkaTopic = readFromENV("KAFKA_TOPIC", "default")

	fmt.Println("Kafka Broker - ", kafkaServer)
	fmt.Println("Kafka topic - ", kafkaTopic)
}

func main() {

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

	log.Print(result)

	var producer *kafka.Producer
	var err error

	// try to connect until we succeed? TODO replace with set num of connection attempts
	for {

		producer, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaServer})

		if err != nil {
			log.Fatalf("Failed to create producer: %s", err)
		}

		if producer != nil {
			break
		}

	}

	value, marshalErr := json.Marshal(result.Vulnerabilities[0])
	if marshalErr != nil {
		log.Fatalf("Failed to serialize CVE data into JSON: %s", marshalErr)
	}

	producerErr := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
		Value:          []byte(value),
	}, nil)

	if producerErr != nil {
		fmt.Printf("Failed to enqueue message %s: %s", value, producerErr)
	}
	event := <-producer.Events() // dequeue an entry from the Events channel

	message, ok := event.(*kafka.Message)

	if !ok {
		log.Fatalf("Channel returned kafka.Error, message %s likely to have failed to enqueue: %s", value, event)
	}

	if message.TopicPartition.Error != nil {
		fmt.Println("Delivery failed due to error ", message.TopicPartition.Error)
	} else {
		fmt.Println("Delivered message to offset " + message.TopicPartition.Offset.String() + " in partition " + message.TopicPartition.String())
	}

}

func readFromENV(key, defaultVal string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultVal
	}
	return value
}
