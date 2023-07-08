package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

var kafkaWriter *kafka.Writer

func init() {
	kafkaServer := readFromENV("KAFKA_BROKER", "localhost:9092")
	kafkaTopic := readFromENV("KAFKA_TOPIC", "nvd-cves")

	kafkaWriter = newKafkaWriter(kafkaServer, kafkaTopic)

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

	defer kafkaWriter.Close()

	time.Sleep(10 * time.Second) // TODO this is a temp hack to wait for kafka to start accepting before we have proper connection/retry handling

	// initialize out data set
	// TODO do we actually need to, or has it already been done?

	startIndex := 0
	totalResults := 0

	for done := false; !done; done = (startIndex > totalResults) {

		resp, getErr := http.Get("https://services.nvd.nist.gov/rest/json/cves/2.0?startIndex=" + strconv.Itoa(startIndex))

		if getErr != nil {
			log.Fatalf("HTTP request failed: %s", getErr) // Fatalln prints then exits
		}

		// pull the response body out and deserialize it into a Response obj
		body, ioReadErr := io.ReadAll(resp.Body)
		if ioReadErr != nil {
			log.Fatalf("Failed to read body from HTTP response: %s", ioReadErr)
		}

		var result Response
		if unmarshalErr := json.Unmarshal(body, &result); unmarshalErr != nil {
			log.Fatalf("Failed to parse response body into JSON: %s", unmarshalErr)
		}

		// send each of the CVE data elements to kafka

		for i := 0; i < len(result.Vulnerabilities); i++ {

			value, marshalErr := json.Marshal(result.Vulnerabilities[i])
			if marshalErr != nil {
				log.Fatalf("Failed to serialize CVE data into JSON: %s", marshalErr)
			}

			key := result.Vulnerabilities[i].Cve.ID
			msg := kafka.Message{
				Key:   []byte(key),
				Value: []byte(value),
			}
			writerError := kafkaWriter.WriteMessages(context.Background(), msg)
			if writerError != nil {
				fmt.Printf("Failed to enqueue message %s: %s\n", value, writerError)
			} else {
				fmt.Printf("Enqueued data for CVE %s\n", key)
			}

		}

		totalResults = result.TotalResults
		startIndex = startIndex + result.ResultsPerPage

		time.Sleep(5 * time.Second)

	}

	// TODO how do we persist the fact that initialization is complete?

}

func readFromENV(key, defaultVal string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultVal
	}
	return value
}
