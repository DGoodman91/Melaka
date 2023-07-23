package main

import (
	"fmt"
	"log"
	"time"
)

type App struct {
	Api ApiScraper
}

func (a *App) Run() error {

	fmt.Println("Starting NVD Scraper app...")

	// TODO If the dataset has been initialized, skip this
	err := a.Api.FetchAll()
	if err != nil {
		return err
	}

	err = a.Api.StartPolling()
	if err != nil {
		return err
	}

	return nil

}

func main() {

	time.Sleep(10 * time.Second) // TODO this is a temp hack to wait for kafka to start accepting before we have proper connection/retry handling

	// create new kafkahandler instance
	var cveHandler *KafkaHandler = newKafkaHandler(readFromENV("KAFKA_BROKER", "localhost:9092"), readFromENV("KAFKA_TOPIC", "nvd-cves"))
	defer cveHandler.Writer.Close()

	// create new nvdscraper instance and wire in kafkahandler
	var scraper ApiScraper = NewNvdApiScraper(cveHandler, readFromENV("NVD_API_KEY", ""))

	// create new app instance and wire in nvdscraper
	app := App{
		Api: scraper,
	}
	if err := app.Run(); err != nil {
		log.Fatalf("Error running app: %s", err)
	}

}
