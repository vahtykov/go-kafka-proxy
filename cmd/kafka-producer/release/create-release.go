package main

import (
	"encoding/json"
	"fmt"
	"go-rest-api-kafka/internal/config"
	"log"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/brianvoe/gofakeit"
	"github.com/google/uuid"
)

type CI struct {
	Code string `json:"code"`
	Name string `json:"name"`
	Type string `json:"type"`
}

type ItService struct {
	Code string `json:"code"`
	Name string `json:"name"`
	Type string `json:"type"`
}

type UatOrganizer struct {
	FirstName string `json:"firstName"`
	LastName string `json:"lastName"`
	MiddleName string `json:"middleName"`
	Email string `json:"email"`
}

type Payload struct {
    Code        string      `json:"code"`
    Status      string      `json:"status"`
		ReleaseType *string      `json:"releaseType"`
		WithoutIntegration *string `json:"withoutIntegration"`
		CanaryType *string `json:"canaryType"`
		Omni *string `json:"omni"`
		Model *string `json:"model"`
		Ci []CI `json:"ci"`
		ItService []ItService `json:"itService"`
		DistrLink []string `json:"distrLink"`
		Risk *string `json:"risk"`
		IssueKey *string `json:"issueKey"`
		IssueId *string `json:"issueId"`
		DomainJira *string `json:"domainJira"`
		CreatedAt time.Time `json:"createdAt"`
		UpdatedAt time.Time `json:"updatedAt"`
		UatOrganizer UatOrganizer `json:"uatOrganizer"`
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

	// Создаем тестовый релиз
	release := map[string]interface{}{
		"id": uuid.New().String(),
		"system": "Track",
		"eventDate": time.Now(),
		"eventType": "CREATE",
		"suitCode": "release",
		"spaceCode": "RELS",
		"payload": Payload{
			Code: "AAA-301",
			Status: "Формирование",
			ReleaseType: nil,
			WithoutIntegration: nil,
			CanaryType: nil,
			Omni: nil,
			Model: nil,
			Ci: []CI{
				{
					Code: "00001",
					Name: "Test",
					Type: "Test type",
				},
			},
			ItService: []ItService{
				{
					Code: "00001",
					Name: "Test",
					Type: "Test type",
				},
			},
			DistrLink: []string{},
			Risk: nil,
			IssueKey: nil,
			IssueId: nil,
			DomainJira: nil,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			UatOrganizer: UatOrganizer{
				FirstName: gofakeit.FirstName(),
				LastName: gofakeit.LastName(),
				MiddleName: gofakeit.Name(),
				Email: gofakeit.Email(),
			},
		},
	}

	// Сериализуем в JSON
	rlsJSON, err := json.Marshal(release)
	if err != nil {
		log.Fatalf("Failed to marshal release: %v", err)
	}

	// Отправляем сообщение
	msg := &sarama.ProducerMessage{
		Topic: cfg.KafkaTopic,
		Value: sarama.StringEncoder(rlsJSON),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)
}