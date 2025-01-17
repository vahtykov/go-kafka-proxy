package server

import (
	"context"
	"go-rest-api-kafka/internal/handlers"

	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type Server struct {
	router *gin.Engine
	db     *gorm.DB
	httpServer *http.Server
}

func NewServer(db *gorm.DB) *Server {
	router := gin.Default()

	router.GET("/health", handlers.HealthCheck)

	return &Server{
		router: router,
		db:     db,
	}
}

func (s *Server) Start(addr string) error {
	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: s.router,
	}
	
	return s.httpServer.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}