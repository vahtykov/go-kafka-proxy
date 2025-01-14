package main

import (
	"encoding/json"
	"fmt"
	"go-rest-api-kafka/internal/config"
	"log"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/google/uuid"
)

type Responsible struct {
    FirstName  string `json:"firstName"`
    LastName   string `json:"lastName"`
    MiddleName string `json:"middleName"`
    Email      string `json:"email"`
}

type Payload struct {
    Code        string      `json:"code"`
    Status      string      `json:"status"`
    TypeWork    interface{} `json:"typeWork"`
    Release     []string    `json:"release"`
    Responsible Responsible `json:"responsible"`
    Risk        interface{} `json:"risk"`
    MainStep    interface{} `json:"mainStep"`
    DryRunDate  interface{} `json:"dryRunDate"`
    IssueKey    interface{} `json:"issueKey"`
    IssueID     interface{} `json:"issueId"`
    DomainJira  interface{} `json:"domainJira"`
    CreatedAt   time.Time   `json:"createdAt"`
    UpdatedAt   time.Time   `json:"updatedAt"`
}

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
	plan := map[string]interface{}{
    "id":        uuid.New().String(),
    "system":    "Track",
    "eventDate": time.Now(),
    "eventType": "CREATE",
    "suitCode":  "plan",
    "spaceCode": "AAA",
    "payload": Payload{
        Code:   "AAA-301",
        Status: "Формирование",
        Release: []string{"AAA-306"},
        Responsible: Responsible{
            FirstName:  gofakeit.FirstName(),
            LastName:   gofakeit.LastName(),
            MiddleName: gofakeit.Name(), // или другой подходящий генератор
            Email:      gofakeit.Email(),
        },
        TypeWork:   nil,
        Risk:       nil,
        MainStep:   nil,
        DryRunDate: nil,
        IssueKey:   nil,
        IssueID:    nil,
        DomainJira: nil,
        CreatedAt:  time.Now(),
        UpdatedAt:  time.Now(),
		},
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