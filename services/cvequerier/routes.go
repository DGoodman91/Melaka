package main

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

type Server struct {
	db     DBConnector
	router *gin.Engine
}

func buildServer(database DBConnector) *Server {

	engine := gin.Default()
	engine.SetTrustedProxies(nil)

	var s Server = Server{
		db:     database,
		router: engine,
	}

	// map routes
	engine.GET("/cve/:id", s.getCve)

	return &s

}

func (s *Server) getCve(c *gin.Context) {

	id := c.Param("id")
	log.Printf("CVE %s requested", id)

	cve, err := s.db.GetCveFromID(id)
	if err != nil {
		// TODO need to add some error middleware to our API
	}
	c.IndentedJSON(http.StatusOK, cve)

}
