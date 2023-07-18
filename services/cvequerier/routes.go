package main

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

func buildRouter() *gin.Engine {

	router := gin.Default()
	router.SetTrustedProxies(nil)

	// map routes
	router.GET("/cve/:id", getCve)

	return router

}

func getCve(c *gin.Context) {

	id := c.Param("id")
	log.Printf("CVE %s requested", id)

	cve, err := db.GetCveFromID(id)
	if err != nil {
		// TODO need to add some error middleware to our API
	}
	c.IndentedJSON(http.StatusOK, cve)

}
