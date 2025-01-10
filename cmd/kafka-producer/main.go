package main

import (
	"encoding/json"
	"fmt"
	"go-rest-api-kafka/internal/config"
	"go-rest-api-kafka/internal/models"
	"log"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/brianvoe/gofakeit/v6"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true


	// TODO: добавить создание группы, и запись в нее
	producer, err := sarama.NewSyncProducer(strings.Split(cfg.KafkaBrokers, ","), config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Создаем тестовый план
	plan := models.Plan{
		ID:          int(gofakeit.Uint16()),
		Name:        gofakeit.BeerName(),
		Description: gofakeit.Sentence(10),
		Data:        fmt.Sprintf(`{"key": "%s", "value": "%s", "timestamp": "%s"}`, 
			gofakeit.Word(), 
			gofakeit.Word(),
			time.Now().Format(time.RFC3339),
		),
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	// Сериализуем план в JSON
	planJSON, err := json.Marshal(plan)
	if err != nil {
		log.Fatalf("Failed to marshal plan: %v", err)
	}

	// Отправляем сообщение
	msg := &sarama.ProducerMessage{
		Topic: cfg.KafkaTopic,
		Value: sarama.StringEncoder(planJSON),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)
}