package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	kafkaWriter *kafka.Writer
	httpClient  *http.Client
	nvdAPIKey   string
	wg          sync.WaitGroup
)

const batchSize = 1000    // Using this as a simple way to work around the VSCode debugger's 1024 limit on goroutines :@
const maxHTTPRetries = 10 // For NVD HTTP requests

func init() {
	kafkaServer := readFromENV("KAFKA_BROKER", "localhost:9092")
	kafkaTopic := readFromENV("KAFKA_TOPIC", "nvd-cves")

	kafkaWriter = newKafkaWriter(kafkaServer, kafkaTopic)
	fmt.Println("Kafka Broker - ", kafkaServer)
	fmt.Println("Kafka Topic - ", kafkaTopic)

	httpClient = &http.Client{}

	nvdAPIKey = readFromENV("NVD_API_KEY", "")
}

func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL), // TODO handling for multiple brokers
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func main() {
	defer kafkaWriter.Close()
	time.Sleep(10 * time.Second) // TODO this is a temp hack to wait for kafka to start accepting before we have proper connection/retry handling

	startIndex := 0
	totalResults := 0

	// TODO how do we persist the fact that initialization is complete?
	// TODO once initialization is complete, need to start periodically checking for updates using the lastModifiedDate parameter
	// NVD docs suggest doing this no more than once every two hours - could be better served by a serverless function that runs sporadically instead of a long-running one

	for startIndex <= totalResults {
		resp, err := sendHTTPGetRequest(fmt.Sprintf("https://services.nvd.nist.gov/rest/json/cves/2.0?startIndex=%d&resultsPerPage=%d", startIndex, batchSize), maxHTTPRetries)
		if err != nil {
			log.Fatalf("HTTP request failed: %s", err)
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Fatalf("Failed to read body from HTTP response: %s", err)
		}

		// Pull the response body out and deserialize it into a Response obj
		var result Response
		if err := json.Unmarshal(body, &result); err != nil {
			log.Fatalf("Failed to parse response body into JSON: %s", err)
		}

		// Send each of the CVE data elements to kafka
		for _, vulnerability := range result.Vulnerabilities {
			cve := vulnerability.Cve
			cveMsg, err := NewCveMsg(cve, result.Timestamp)
			if err != nil {
				log.Printf("Error generating CveMsg instance for data %s: %s", cve.ID, err)
				continue
			}

			value, err := json.Marshal(cveMsg)
			if err != nil {
				log.Printf("Failed to serialize CVE data into JSON: %s", err)
				continue
			}

			key := cve.ID
			msg := kafka.Message{
				Key:   []byte(key),
				Value: []byte(value),
			}

			// Increment the wait group before starting the goroutine
			wg.Add(1)
			go enqueueMessage(msg)

		}

		// Wait for all goroutines to finish
		wg.Wait()

		log.Printf("Batch with startIndex %d complete", startIndex)

		totalResults = result.TotalResults
		startIndex += result.ResultsPerPage

		time.Sleep(10 * time.Second) // wait to avoid hitting NVD API limits
	}

}

func enqueueMessage(msg kafka.Message) {

	defer wg.Done() // Decrement the WaitGroup when the goroutine completes

	// Write the message to Kafka
	if err := kafkaWriter.WriteMessages(context.Background(), msg); err != nil {
		log.Printf("Failed to enqueue message %s: %s\n", msg.Value, err)
	} else {
		log.Printf("Enqueued data for %s\n", msg.Key)
	}
}
