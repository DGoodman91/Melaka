package main

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Runnable interface {
	Run(addr string) error
}

type Server struct {
	db     DBConnector
	router *gin.Engine
}

func (s *Server) Run(addr string) error {

	if err := s.router.Run(addr); err != nil {
		return err
	}

	return nil
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
	engine.GET("/metrics", gin.WrapH(promhttp.Handler()))

	return &s

}

func (s *Server) getCve(c *gin.Context) {

	id := c.Param("id")
	log.Printf("CVE %s requested", id)

	cve, err := s.db.GetCveFromID(id)
	if err != nil {
		// TODO need to add some error middleware to our API
		log.Printf("Instantiation of db connection failed: %s", err)
	}
	c.IndentedJSON(http.StatusOK, cve)

}
