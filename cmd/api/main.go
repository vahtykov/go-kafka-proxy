package main

import (
	"context"
	"fmt"
	"go-kafka-proxy/internal/config"
	"go-kafka-proxy/internal/database"
	"go-kafka-proxy/internal/kafka"
	"go-kafka-proxy/internal/server"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Загрузка конфигурации
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Подключение к базе данных
	db, err := database.NewPostgresDB(cfg)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Создание Kafka консьюмер
	consumer, err := kafka.NewConsumer(
		cfg.KafkaBrokers,
		cfg.KafkaTopic,
		cfg.KafkaGroupID,
		db,
		cfg.ServiceLoaderURL,
		cfg,
	)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}

	// Запускаем консьюмер
	if err := consumer.Start(); err != nil {
		log.Fatalf("Failed to start Kafka consumer: %v", err)
	}

	// Создание HTTP сервера
	srv := server.NewServer(db)

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Запускаем сервер в отдельной горутине
	go func() {
		if err := srv.Start(fmt.Sprintf(":%s", cfg.Port)); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Ожидаем сигнал остановки
	<-sigChan

	// Создаем контекст с таймаутом для graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Сначала останавливаем консьюмер
	if err := consumer.Close(); err != nil {
		log.Printf("Error closing consumer: %v", err)
	}

	// Затем останавливаем HTTP сервер
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("Error shutting down server: %v", err)
	}
}