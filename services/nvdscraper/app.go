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
var httpClient *http.Client
var nvdAPIKey string

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

		// TODO retry handling
		req, reqErr := http.NewRequest("GET", "https://services.nvd.nist.gov/rest/json/cves/2.0?startIndex="+strconv.Itoa(startIndex), nil)
		if reqErr != nil {
			log.Fatalf("HTTP request creation failed: %s", reqErr) // Fatalln prints then exits
		}

		if nvdAPIKey != "" {
			req.Header.Add("apiKey", nvdAPIKey)
		}

		resp, getErr := httpClient.Do(req)
		if getErr != nil {
			log.Fatalf("HTTP request failed: %s", getErr) // TODO need retries, not built into golang's http.client though https://medium.com/@nitishkr88/http-retries-in-go-e622e51d249f
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			respMsg := ""
			val, ok := resp.Header["Message"]
			if ok && len(val) > 0 {
				respMsg = resp.Header["Message"][0]
			}
			log.Fatalf("Unexpected response from NVD, response code %s, message %s", strconv.Itoa(resp.StatusCode), respMsg)
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

			cve := result.Vulnerabilities[i].Cve

			cveMsg, cveMsgErr := NewCveMsg(cve, result.Timestamp)
			if cveMsgErr != nil {
				log.Printf("Error generating CveMsg instance for data %s, %s", cve.ID, cveMsgErr)
				continue
			}

			value, marshalErr := json.Marshal(cveMsg)
			if marshalErr != nil {
				log.Printf("Failed to serialize CVE data into JSON: %s", marshalErr)
				continue
			}

			key := result.Vulnerabilities[i].Cve.ID
			msg := kafka.Message{
				Key:   []byte(key),
				Value: []byte(value),
			}
			writerError := kafkaWriter.WriteMessages(context.Background(), msg)
			if writerError != nil {
				fmt.Printf("Failed to enqueue message %s: %s\n", value, writerError)
				continue
			} else {
				fmt.Printf("Enqueued data for CVE %s\n", key)
			}

		}

		totalResults = result.TotalResults
		startIndex = startIndex + result.ResultsPerPage

		time.Sleep(10 * time.Second)

	}

	// TODO how do we persist the fact that initialization is complete?

	// TODO once initialization is complete, need to start periodically checking for updates using the lastModifiedDate parameter
	// NVD docs suggest doing this no more than once every two hours - could be better served by a serverless function that runs sporadically instead of a long-running one

}

func readFromENV(key, defaultVal string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultVal
	}
	return value
}
