package main

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type DBConnector interface {
	Connect() error
	GetCveFromID(id string) (interface{}, error)
}

// Define our MongoDB type and implement the DBConnector interface on it

type MongoDB struct {
	Configuration DBConnConfig
	Connection    *mongo.Client
	Database      *mongo.Database
	CveCollection *mongo.Collection
}

func (m *MongoDB) Connect() error {

	mongoServer := m.Configuration.Url

	credentials := options.Credential{
		Username: m.Configuration.Username,
		Password: m.Configuration.Password,
	}

	var err error
	m.Connection, err = mongo.Connect(context.TODO(), options.Client().ApplyURI(mongoServer).SetAuth(credentials))
	if err != nil {
		log.Fatalf("Instantiation of db connection failed: %s", err)
	}

	// Ping db to test the connection
	if err := m.Connection.Ping(context.TODO(), readpref.Primary()); err != nil {
		log.Fatalf("Pinging db failed: %s", err)
	}

	// Ready our mongodb collection for access
	m.Database = m.Connection.Database(m.Configuration.Database)
	m.CveCollection = m.Database.Collection(m.Configuration.CveCollection)

	return nil

}

func (db *MongoDB) GetCveFromID(id string) (interface{}, error) {

	filter := bson.D{{"cvedata.id", id}}

	var result CveMsg
	err := db.CveCollection.FindOne(context.TODO(), filter).Decode(&result)
	if err != nil {
		return nil, err
	}

	return result, nil

}

// An object to hold our connection config for databases

type DBConnConfig struct {
	Url           string
	Username      string
	Password      string
	Database      string
	CveCollection string
}
