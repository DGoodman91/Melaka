package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCveGetHandler(t *testing.T) {

	router := buildRouter()

	req, err := http.NewRequest("GET", "/cve/CVE-0000-0000", nil)
	if err != nil {
		t.Fatal(err)
	}

	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)

	assert.Equal(t, http.StatusOK, resp.Code)
	//assert.JSONEq(t, `{"message": "Hello, World!"}`, resp.Body.String())

}
