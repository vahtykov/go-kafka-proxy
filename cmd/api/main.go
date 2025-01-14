package main

import (
	"fmt"
	"go-rest-api-kafka/internal/config"
	"go-rest-api-kafka/internal/database"
	"go-rest-api-kafka/internal/kafka"
	"go-rest-api-kafka/internal/server"
	"log"
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
	)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}

	// Запуск консьюмера
	if err := consumer.Start(); err != nil {
		log.Fatalf("Failed to start Kafka consumer: %v", err)
	}

	// Создание и запуск HTTP сервера
	srv := server.NewServer(db)
	if err := srv.Start(fmt.Sprintf(":%s", cfg.Port)); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}