package main

import (
	"log"

	"github.com/gin-gonic/gin"
)

var (
	db     DBConnector
	router *gin.Engine
)

func init() {

	// Create a DB instance and connect to our database

	db = &MongoDB{
		Configuration: DBConnConfig{
			Url:           readFromENV("MONGO_URL", "mongodb://localhost:27017"),
			Username:      readFromENV("MONGO_ROOT_USERNAME", "dev"),
			Password:      readFromENV("MONGO_ROOT_PASSWORD", "dev"),
			Database:      readFromENV("MONGO_DATABASE", "melaka"),
			CveCollection: readFromENV("MONGO_CVE_COLLECTION", "cves"),
		},
	}

	err := db.Connect()
	if err != nil {
		log.Fatalf("Failed to connect to DB instance with error: %s", err)
	}

	// Set up our server with it's routes & middleware
	router = buildRouter()

}

func main() {

	// Run our server!
	router.Run(":8080")

}
