package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

const batchSize = 1000    // Using this as a simple way to work around the VSCode debugger's 1024 limit on goroutines :@
const maxHTTPRetries = 10 // For NVD HTTP requests

type ApiScraper interface {
	FetchAll() error
	StartPolling() error
	Close() error
}

type NvdApiScraper struct {
	Handler    CveHandler
	httpClient *http.Client
	apiKey     string
	wg         sync.WaitGroup
}

func (n *NvdApiScraper) FetchAll() error {

	startIndex := 0
	totalResults := 0

	for startIndex <= totalResults {
		resp, err := n.sendHTTPGetRequest(fmt.Sprintf("https://services.nvd.nist.gov/rest/json/cves/2.0?startIndex=%d&resultsPerPage=%d", startIndex, batchSize), maxHTTPRetries)
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

			err = n.Handler.WriteCve(cveMsg, &n.wg)
			if err != nil {
				fmt.Printf("Error writing CVE data: %s\n", err)
				continue
			}

		}

		// Wait for all goroutines to finish
		n.wg.Wait()

		log.Printf("Batch with startIndex %d complete", startIndex)

		totalResults = result.TotalResults
		startIndex += result.ResultsPerPage

		time.Sleep(10 * time.Second) // wait to avoid hitting NVD API limits
	}

	// TODO how do we persist the fact that initialization is complete?

	return nil
}

func (n *NvdApiScraper) StartPolling() error {

	// TODO once initialization is complete, need to start periodically checking for updates using the lastModifiedDate parameter
	// NVD docs suggest doing this no more than once every two hours - could be better served by a serverless function that runs sporadically instead of a long-running one

	return nil
}

func (n *NvdApiScraper) Close() error {
	n.Handler.Close()
	return nil
}

func NewNvdApiScraper(handler CveHandler, key string) *NvdApiScraper {
	return &NvdApiScraper{
		Handler:    handler,
		httpClient: &http.Client{},
		apiKey:     key,
	}
}

func (n *NvdApiScraper) sendHTTPGetRequest(url string, maxRetries int) (*http.Response, error) {
	var resp *http.Response
	var err error

	for retry := 0; retry < maxRetries; retry++ {
		resp, err = n.sendGetRequest(url)
		if err == nil && resp.StatusCode == http.StatusOK {
			return resp, nil
		}

		log.Printf("HTTP request to %s failed. Response code %d, Error: %s \nRetrying... (retry %d/%d)", url, resp.StatusCode, err, retry+1, maxRetries)
		time.Sleep(5 * time.Second)

	}

	return nil, fmt.Errorf("unexpected response from nvd, response code %d, message: %s", resp.StatusCode, resp.Header.Get("Message"))
}

func (n *NvdApiScraper) sendGetRequest(url string) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	if n.apiKey != "" {
		req.Header.Add("apiKey", n.apiKey)
	}

	return n.httpClient.Do(req)
}
