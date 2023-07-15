package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var (
	kafkaReader  *kafka.Reader
	dbCollection *mongo.Collection
	wg           sync.WaitGroup
	maxWorkers   = 10 // Maximum number of concurrent goroutines
)

func init() {
	// Connect to Kafka broker
	kafkaServer := readFromENV("KAFKA_BROKER", "localhost:9092")
	kafkaTopic := readFromENV("KAFKA_TOPIC", "nvd-cves")
	kafkaReader = newKafkaReader(kafkaServer, kafkaTopic)
	fmt.Println("Kafka Broker - ", kafkaServer)
	fmt.Println("Kafka Topic - ", kafkaTopic)

	// Connect to MongoDB
	mongoServer := readFromENV("MONGO_URL", "mongodb://localhost:27017")
	mongoDatabaseName := readFromENV("MONGO_DB", "melakaDB")
	mongoCollectionName := readFromENV("MONGO_COLLECTION", "cves")

	credentials := options.Credential{
		Username: readFromENV("MONGO_ROOT_USERNAME", "dev"),
		Password: readFromENV("MONGO_ROOT_PASSWORD", "dev"),
	}

	dbClient, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(mongoServer).SetAuth(credentials))
	if err != nil {
		log.Fatalf("Instantiation of db connection failed: %s", err)
	}

	// Ping db to test the connection
	if err := dbClient.Ping(context.TODO(), readpref.Primary()); err != nil {
		log.Fatalf("Pinging db failed: %s", err)
	}

	dbCollection = dbClient.Database(mongoDatabaseName).Collection(mongoCollectionName)
}

func main() {
	defer kafkaReader.Close()

	// Create a channel to limit the number of goroutines
	workerChan := make(chan struct{}, maxWorkers)

	for {
		m, err := kafkaReader.ReadMessage(context.Background())
		if err != nil {
			continue
		}

		// Acquire a worker from the channel
		workerChan <- struct{}{}

		// Increment the WaitGroup for each message
		wg.Add(1)

		// Start a goroutine to handle the Kafka message
		go func(msg kafka.Message) {
			defer func() {
				<-workerChan // Release the worker back to the channel
				wg.Done()    // Decrement the WaitGroup when the goroutine completes
			}()

			if err := handleMsg(msg); err != nil {
				fmt.Printf("Failed to handle message: %s", err)
			}
		}(m)
	}

}

func handleMsg(msg kafka.Message) error {
	fmt.Printf("Read message from broker, key %s, value %s", string(msg.Key), string(msg.Value))

	var bdoc interface{}
	if err := bson.UnmarshalExtJSON(msg.Value, false, &bdoc); err != nil {
		return err
	}

	result, err := dbCollection.InsertOne(context.TODO(), bdoc)
	if err != nil {
		return err
	}

	fmt.Printf("Inserted record with ID %s", result.InsertedID)
	return nil
}

func newKafkaReader(kafkaURL, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{kafkaURL}, // TODO handling for multiple brokers
		GroupID:   "CVE-Writers",
		Topic:     topic,
		Partition: 0,
		MaxBytes:  10e6,
	})
}

func readFromENV(key, defaultVal string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultVal
}
