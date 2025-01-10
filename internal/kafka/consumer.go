package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"go-rest-api-kafka/internal/models"
	"net/http"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"gorm.io/gorm"
)

type Consumer struct {
	consumerGroup sarama.ConsumerGroup
	db           *gorm.DB
	topic        string
	serviceURL   string
	httpClient   *http.Client
	ready        chan bool
}

/**
	Цепочка вызовов при работе с консьюмером Kafka (нормальная работа):
	Start()  // Запуск консьюмера, вызов Consume()
  └─ Setup()                  // Инициализация (Kafka автоматом вызывает Setup() после вызова Consume())
     └─ ConsumeClaim()        // Чтение сообщений (после Setup() Kafka вызывает ConsumeClaim())
        └─ ConsumeClaim()     // Продолжаем читать
           └─ ConsumeClaim()  // И так далее...

	При перебалансировке:
	... ConsumeClaim()  // Работаем
   └─ Cleanup()     // Останавливаемся
      └─ Setup()    // Реинициализация
         └─ ConsumeClaim()  // Продолжаем работу
*/

func NewConsumer(brokers string, topic string, groupID string, db *gorm.DB, serviceURL string) (*Consumer, error) {
	config := sarama.NewConfig()
	
	// Настройки группы
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest  // earliest - Начинаем читать с самого начала
	config.Consumer.Offsets.AutoCommit.Enable = false      // disable auto commit - Сами контролируем подтверждение
	config.Consumer.MaxProcessingTime = 300 * time.Second  // max.poll.interval.ms
	config.Consumer.Group.Session.Timeout = 60 * time.Second
	config.Consumer.MaxWaitTime = 500 * time.Millisecond
	config.Consumer.Fetch.Max = 100                        // max.poll.records - Читаем по 100 сообщений за раз

	group, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), groupID, config)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		consumerGroup: group,
		db:           db,
		topic:        topic,
		serviceURL:   serviceURL,
		httpClient:   &http.Client{
			Timeout: 10 * time.Second, // Максимальное время ожидания ответа
		},
		ready:        make(chan bool),
	}, nil
}

// Вызывается при старте потребителя (Реализация интерфейса sarama.ConsumerGroupHandler)
func (c *Consumer) Setup(_ sarama.ConsumerGroupSession) error {
	close(c.ready) // Закрываем канал, что разблокирует все ожидающие горутины
	return nil
}

// Вызывается при остановке потребителя
func (c *Consumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	c.ready = make(chan bool) // Готовим новый канал для обработки следующего сообщения
	return nil
}

// Вызывается для обработки сообщений
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var plan models.Plan

		// 1. Читаем сообщение
		if err := json.Unmarshal(msg.Value, &plan); err != nil {
			fmt.Printf("Failed to unmarshal message: %v\n", err)
			continue
		}

		// 2. Сохраняем в БД
		if err := c.db.Create(&plan).Error; err != nil {
			fmt.Printf("Failed to save to database: %v\n", err)
			continue
		}

		// 3. Ручной commit после успешной обработки
		session.MarkMessage(msg, "")
	}
	return nil
}

func (c *Consumer) Start() error {
	ctx := context.Background()
	topics := []string{c.topic}

	go func() {
		for {
			// Читаем сообщения пока контекст активен
			if err := c.consumerGroup.Consume(ctx, topics, c); err != nil {
				fmt.Printf("Error from consumer: %v\n", err)
			}
			// Проверяем, был ли контекст отменен
			if ctx.Err() != nil {
				return // Контекст отменен, завершаем горутину
			}

			// Создаём НОВЫЙ канал для следующего цикла
			c.ready = make(chan bool)
		}
	}()

	// Ждём, пока консьюмер будет готов
	<-c.ready // Блокируемся здесь, пока канал не закроется
	fmt.Println("Consumer is ready")

	return nil
}