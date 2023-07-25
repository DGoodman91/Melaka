package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Create a type that implements DBConnector so we can mock our db requests
type MockDatabase struct{}

func (m *MockDatabase) Connect() error {
	return nil
}

func (m *MockDatabase) GetCveFromID(id string) (interface{}, error) {
	return CveMsg{CveData: NvdCveData{ID: id}}, nil
}

func TestCveGetHandler(t *testing.T) {

	m := &MockDatabase{}

	server := buildServer(m)

	id := "CVE-0000-0000"

	req, err := http.NewRequest("GET", "/cve/"+id, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp := httptest.NewRecorder()
	server.router.ServeHTTP(resp, req)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Contains(t, resp.Body.String(), id)

}
