package main

import (
	"os"
)

// readFromENV retrieves the value of the environment variable specified by the key.
// If the value is not empty, it is returned. Otherwise, the default value is returned.
func readFromENV(key, defaultVal string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultVal
}
