package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/your-project/database"
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

type Setting struct {
    ID        uint   `gorm:"primarykey;column:id;"`
    Name      string `gorm:"not null;column:name;"`
    Service   string `gorm:"not null;column:service;"`
    ValueType string `gorm:"column:value_type;not null;column:value_type;"`
    Value     string `gorm:"column:value;not null;column:value;"`
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

	Общая схема:
	Время ──────────────────────────────────►
		1. Start()      ─┐
		2. Setup()       ├─ Начальный запуск
		3. ConsumeClaim()┘

		4. ConsumeClaim() ─┐
		5. ConsumeClaim()  ├─ Нормальная работа
		6. ConsumeClaim() ─┘

		7. Cleanup()      ─┐
		8. Setup()         ├─ Перебалансировка
		9. ConsumeClaim() ─┘
*/

func GetKafkaConsumerStrategy(db *gorm.DB) (string, error) {
    var setting Setting
    result := db.Where("service = ? AND name = ?", "kafka_proxy", "kafka_consumer_strategy").First(&setting)
    
    if result.Error != nil {
        if result.Error == gorm.ErrRecordNotFound {
            return "READ_NEWEST_RECORDS", nil // значение по умолчанию
        }
        return "", fmt.Errorf("error fetching kafka consumer strategy: %v", result.Error)
    }
    
    return setting.Value, nil
}

func NewConsumer(brokers string, topic string, groupID string, db *gorm.DB, serviceURL string) (*Consumer, error) {
	config := sarama.NewConfig()
	
	// Получаем стратегию из базы данных
	strategy, err := database.GetKafkaConsumerStrategy(db)
	if err != nil {
		return nil, fmt.Errorf("error getting kafka consumer strategy: %v", err)
	}
	
	// Настройки группы
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	
	// Устанавливаем стратегию чтения на основе значения из БД.
	switch strategy {
	case "READ_FROM_BEGINNING":
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	case "READ_NEWEST_RECORDS":
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
		return nil, fmt.Errorf("unknown kafka consumer strategy: %s", strategy)
	}
	
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.MaxProcessingTime = 300 * time.Second
	config.Consumer.Group.Session.Timeout = 60 * time.Second
	config.Consumer.MaxWaitTime = 500 * time.Millisecond
	config.Consumer.Fetch.Max = 100 // max.poll.records - Читаем по 100 сообщений за раз

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
			Timeout: 10 * time.Second,
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

func (c *Consumer) sendToLoader(messageData []byte) error {
	// Декодируем JSON только для получения полей suitCode и eventType
	var message map[string]interface{}
	if err := json.Unmarshal(messageData, &message); err != nil {
		return fmt.Errorf("ошибка декодирования JSON: %v", err)
	}

	// Получаем и приводим к нижнему регистру необходимые поля
	suitCode := strings.ToLower(fmt.Sprint(message["suitCode"]))
	eventType := strings.ToLower(fmt.Sprint(message["eventType"]))

	var (
		method string
		path   string
	)

	switch {
		case suitCode == "release" && eventType == "create":
			method = http.MethodPut
			path = "/release/save"
		case suitCode == "release" && eventType == "update":
			method = http.MethodPatch
			path = "/release/update"
		case suitCode == "plan" && eventType == "create":
			method = http.MethodPut
			path = "/plan/save"
		case suitCode == "plan" && eventType == "update":
			method = http.MethodPatch
			path = "/plan/update"
		default:
			return fmt.Errorf("неподдерживаемая комбинация suitCode=%s и eventType=%s", suitCode, eventType)
	}

	// Формируем URL
	url := fmt.Sprintf("%s%s", c.serviceURL, path)

	// Создаем запрос, используя исходные данные без преобразования
	req, err := http.NewRequest(method, url, bytes.NewBuffer(messageData))
	if err != nil {
		return fmt.Errorf("ошибка создания запроса: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Отправляем запрос
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("ошибка отправки запроса: %v", err)
	}
	defer resp.Body.Close()

	// Проверяем статус ответа
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("неожиданный код ответа: %d, тело: %s", resp.StatusCode, string(body))
	}

	return nil
}

// Вызывается для обработки сообщений
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		// TODO: добавить сохранение сырых данных в базу track_loads_log

		// Отправляем сырые данные в loader
		if err := c.sendToLoader(msg.Value); err != nil {
			fmt.Printf("Ошибка отправки в loader: %v\n", err)
			continue
		}

		fmt.Printf("Сообщение успешно обработано\n")
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

			// Если произошла перебалансировка:
      // - Старый канал уже закрыт
      // - Нужен новый канал для следующего цикла
			c.ready = make(chan bool)
		}
	}()

	// Ждём, пока консьюмер будет готов
	<-c.ready // Блокируемся здесь, пока канал не закроется
	fmt.Println("Consumer is ready")

	return nil
}