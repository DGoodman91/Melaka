package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/segmentio/kafka-go"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var kafkaReader *kafka.Reader
var dbClient *mongo.Client
var dbCollection *mongo.Collection

func init() {

	// connect to Kafka broker

	kafkaServer := readFromENV("KAFKA_BROKER", "localhost:9092")
	kafkaTopic := readFromENV("KAFKA_TOPIC", "nvd-cves")

	kafkaReader = newKafkaReader(kafkaServer, kafkaTopic)

	fmt.Println("Kafka Broker - ", kafkaServer)
	fmt.Println("Kafka Topic - ", kafkaTopic)

	// connect to mongodb

	mongoServer := readFromENV("MONGO_URL", "mongodb://localhost:27017")
	mongoDatabaseName := readFromENV("MONGO_DB", "melaka")
	mongoCollectionName := readFromENV("MONGO_COLLECTION", "cves")

	credentials := options.Credential{
		Username: readFromENV("MONGO_ROOT_USERNAME", "dev"),
		Password: readFromENV("MONGO_ROOT_PASSWORD", "dev"),
	}

	var connErr error

	dbClient, connErr = mongo.Connect(context.TODO(), options.Client().ApplyURI(mongoServer).SetAuth(credentials))
	if connErr != nil {
		log.Fatalf("Instantiation of db connection failed, %s", connErr)
	}

	// ping db to test the connection
	if pingErr := dbClient.Ping(context.TODO(), readpref.Primary()); pingErr != nil {
		log.Fatalf("Pinging db failed, %s", pingErr)
	}

	// set up our collection
	dbCollection = dbClient.Database(mongoDatabaseName).Collection(mongoCollectionName)

}

func main() {

	defer kafkaReader.Close()

	for {
		m, readErr := kafkaReader.ReadMessage(context.Background())
		if readErr != nil {
			continue
		}
		//fmt.Printf("Message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))

		e := handleMsg(m)
		if e != nil {
			fmt.Printf("Failed to handle message, %s", e)
		}
	}

}

func handleMsg(msg kafka.Message) error {

	fmt.Printf("Read message from broker, key %s, value %s", string(msg.Key), string(msg.Value))

	var bdoc interface{}
	unmarshalErr := bson.UnmarshalExtJSON(msg.Value, false, &bdoc)

	if unmarshalErr != nil {
		return unmarshalErr
	}
	result, insertErr := dbCollection.InsertOne(context.TODO(), &bdoc)

	if insertErr != nil {
		return insertErr
	}

	fmt.Printf("%s", result.InsertedID)

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
	value := os.Getenv(key)
	if value == "" {
		return defaultVal
	}
	return value
}
