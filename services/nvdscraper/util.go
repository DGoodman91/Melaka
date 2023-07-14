package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

func sendHTTPGetRequest(url string) (*http.Response, error) {
	var resp *http.Response
	var err error

	for retry := 0; retry < maxHTTPRetries; retry++ {
		resp, err = sendGetRequest(url)
		if err == nil && resp.StatusCode == http.StatusOK {
			return resp, nil
		}

		log.Printf("HTTP request to %s failed. Retrying... (retry %d/%d)", url, retry+1, maxHTTPRetries)
		time.Sleep(5 * time.Second)

	}

	return nil, fmt.Errorf("unexpected response from nvd, response code %d, message: %s", resp.StatusCode, resp.Header.Get("Message"))
}

func sendGetRequest(url string) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	if nvdAPIKey != "" {
		req.Header.Add("apiKey", nvdAPIKey)
	}

	return httpClient.Do(req)
}

// readFromENV retrieves the value of the environment variable specified by the key.
// If the value is not empty, it is returned. Otherwise, the default value is returned.
func readFromENV(key, defaultVal string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultVal
}
