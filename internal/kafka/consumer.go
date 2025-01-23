package kafka

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"go-kafka-proxy/internal/config"
	"go-kafka-proxy/internal/models"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Consumer struct {
	consumerGroup sarama.ConsumerGroup
	db           *gorm.DB
	topic        string
	groupID      string
	serviceURL   string
	httpClient   *http.Client
	ready        chan bool
	processedMessages map[string]struct{} // key = "topic:partition:offset"
  cacheMutex       sync.RWMutex
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

func configureSSL(config *sarama.Config, certDir string) error {
    tlsConfig := &tls.Config{
        InsecureSkipVerify: false,
        MinVersion:         tls.VersionTLS12,
        MaxVersion:         tls.VersionTLS13,
        // Добавляем проверку имени сервера
        VerifyPeerCertificate: func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
            // Пропускаем дополнительную проверку имени хоста
            return nil
        },
    }

    // Читаем сертификат
    certPEM, err := os.ReadFile(filepath.Join(certDir, "tls_pem.txt"))
    if err != nil {
        return fmt.Errorf("error reading certificate: %v", err)
    }

    // Читаем ключ
    keyPEM, err := os.ReadFile(filepath.Join(certDir, "tls.key"))
    if err != nil {
        return fmt.Errorf("error reading private key: %v", err)
    }

    // Читаем корневой сертификат
    caCertPEM, err := os.ReadFile(filepath.Join(certDir, "tls_root_pem.txt"))
    if err != nil {
        return fmt.Errorf("error reading CA certificate: %v", err)
    }

    // Добавляем корневой сертификат
    caCertPool := x509.NewCertPool()
    if !caCertPool.AppendCertsFromPEM(caCertPEM) {
        return fmt.Errorf("failed to parse CA certificate")
    }
    tlsConfig.RootCAs = caCertPool

    // Загружаем сертификат и ключ клиента
    cert, err := tls.X509KeyPair(certPEM, keyPEM)
    if err != nil {
        return fmt.Errorf("error loading certificate and key: %v", err)
    }
    tlsConfig.Certificates = []tls.Certificate{cert}

    // Добавляем ServerName если он есть в сертификате
    if len(cert.Certificate) > 0 {
        if pc, err := x509.ParseCertificate(cert.Certificate[0]); err == nil {
            if len(pc.DNSNames) > 0 {
                tlsConfig.ServerName = pc.DNSNames[0]
            }
        }
    }

    config.Net.TLS.Enable = true
    config.Net.TLS.Config = tlsConfig

    return nil
}

//---Работа с in-memory кэшем---
func (c *Consumer) loadProcessedMessagesCache() error {
    c.cacheMutex.Lock()
    defer c.cacheMutex.Unlock()

    var messages []models.ProcessedMessage
		// TODO: Добавить фильтр, чтобы выгружать только часть сообщений
    if err := c.db.Where("topic = ?", c.topic).Find(&messages).Error; err != nil {
        return err
    }

    c.processedMessages = make(map[string]struct{})
    for _, msg := range messages {
        key := fmt.Sprintf("%s:%d:%d", msg.Topic, msg.Partition, msg.Offset)
        c.processedMessages[key] = struct{}{}
    }
    return nil
}

func (c *Consumer) isMessageProcessedFromCache(msg *sarama.ConsumerMessage) bool {
    c.cacheMutex.RLock()
    defer c.cacheMutex.RUnlock()
    
    key := fmt.Sprintf("%s:%d:%d", msg.Topic, msg.Partition, msg.Offset)
    _, exists := c.processedMessages[key]
    return exists
}

func (c *Consumer) addToProcessedCache(msg *sarama.ConsumerMessage) {
    c.cacheMutex.Lock()
    defer c.cacheMutex.Unlock()
    
    key := fmt.Sprintf("%s:%d:%d", msg.Topic, msg.Partition, msg.Offset)
    c.processedMessages[key] = struct{}{}
}

//---END Работа с in-memory кэшем---

func (c *Consumer) saveOffset(topic string, partition int32, offset int64) error {
    return c.db.Transaction(func(tx *gorm.DB) error {
        var kafkaOffset models.KafkaOffset

        result := tx.Where(
					"topic = ? AND partition = ? AND group_id = ?",
					topic, partition, c.groupID,
				).First(&kafkaOffset)

        if result.Error != nil {
            if result.Error == gorm.ErrRecordNotFound {
                kafkaOffset = models.KafkaOffset{
                    Topic:     topic,
                    Partition: partition,
                    GroupID:   c.groupID,
                }
            } else {
                return result.Error
            }
        }

        kafkaOffset.Offset = offset
        kafkaOffset.UpdatedAt = time.Now()

        return tx.Save(&kafkaOffset).Error
    })
}

func (c *Consumer) isMessageProcessed(msg *sarama.ConsumerMessage) (bool, error) {
    var count int64
    err := c.db.Model(&models.ProcessedMessage{}).Where(
        "topic = ? AND partition = ? AND \"offset\" = ?",
        msg.Topic, msg.Partition, msg.Offset,
    ).Count(&count).Error
    return count > 0, err
}

func (c *Consumer) markMessageProcessed(msg *sarama.ConsumerMessage) error {
    return c.db.Transaction(func(tx *gorm.DB) error {
        // Сохраняем информацию о сообщении
        processedMsg := models.ProcessedMessage{
            MessageID:   string(msg.Key),
            Topic:      msg.Topic,
            Partition:  msg.Partition,
            Offset:     msg.Offset,
            ProcessedAt: time.Now(),
        }
        if err := tx.Create(&processedMsg).Error; err != nil {
            return err
        }

        // Обновляем офсет
        return c.saveOffset(msg.Topic, msg.Partition, msg.Offset)
    })
}

func GetKafkaConsumerStrategy(db *gorm.DB) (string, error) {
    var setting Setting
    result := db.Where(
			"service = ? AND name = ?", "kafka_proxy", "kafka_consumer_strategy",
		).First(&setting)
    
    if result.Error != nil {
        if result.Error == gorm.ErrRecordNotFound {
            return "READ_NEWEST_RECORDS", nil // значение по умолчанию
        }
        return "", fmt.Errorf("error fetching kafka consumer strategy: %v", result.Error)
    }
    
    return setting.Value, nil
}

func NewConsumer(brokers string, topic string, groupID string, db *gorm.DB, serviceURL string, cfg *config.Config) (*Consumer, error) {
	config := sarama.NewConfig()
	
	if cfg.KafkaSSL == "Y" {
		if err := configureSSL(config, cfg.KafkaCertDir); err != nil {
			return nil, fmt.Errorf("failed to configure SSL: %v", err)
		}
	}

	// Получаем стратегию из базы данных
	strategy, err := GetKafkaConsumerStrategy(db)
	if err != nil {
		return nil, fmt.Errorf("error getting kafka consumer strategy: %v", err)
	}
	
	// Настройки группы
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	
	// Устанавливаем стратегию чтения на основе значения из БД
	switch strategy {
	case "READ_FROM_BEGINNING":
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	case "READ_NEWEST_RECORDS":
		fmt.Println("Kafka consumer strategy: READ_NEWEST_RECORDS")
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
		return nil, fmt.Errorf("unknown kafka consumer strategy: %s", strategy)
	}
	
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.MaxProcessingTime = 5 * time.Second
	config.Consumer.Group.Session.Timeout = 20 * time.Second
	config.Consumer.MaxWaitTime = 5 * time.Second
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
			Timeout: 5 * time.Second,
		},
		ready:        make(chan bool),
		groupID:      groupID,
	}, nil
}

// Вызывается при старте потребителя (Реализация интерфейса sarama.ConsumerGroupHandler)
func (c *Consumer) Setup(_ sarama.ConsumerGroupSession) error {
	// Загружаем кэш
	if err := c.loadProcessedMessagesCache(); err != nil {
		fmt.Errorf("Ошибка загрузки кэша: %v\n", err)
	}

	close(c.ready) // Закрываем канал, что разблокирует все ожидающие горутины
	return nil
}

func (c *Consumer) Close() error {
    return c.consumerGroup.Close()
}

// Вызывается при остановке потребителя
func (c *Consumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	c.ready = make(chan bool) // Готовим новый канал для обработки следующего сообщения
	return nil
}

func (c *Consumer) sendToLoader(ctx context.Context, messageData []byte) error {
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
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewBuffer(messageData))
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

func (c *Consumer) logToDatabase(messageData []byte, msg string, op ...string) error {
    var kafkaMessage map[string]interface{}
    if err := json.Unmarshal(messageData, &kafkaMessage); err != nil {
        return fmt.Errorf("ошибка декодирования JSON: %v", err)
    }

		var operation string
    if len(op) > 0 {
        operation = op[0]
    }

    // Извлекаем необходимые поля
    var messageID uuid.UUID
    if id, ok := kafkaMessage["id"].(string); ok {
        parsedUUID, err := uuid.Parse(id)
        if err != nil {
            return fmt.Errorf("ошибка парсинга UUID: %v", err)
        }
        messageID = parsedUUID
    }
    eventType, _ := kafkaMessage["eventType"].(string)
    suitCode, _ := kafkaMessage["suitCode"].(string)

    // Извлекаем payload.code
    payload, ok := kafkaMessage["payload"].(map[string]interface{})
    var code string
    if ok {
        code, _ = payload["code"].(string)
    }

    log := models.TrackLoadLog{
        UUID:      messageID,
        Operation: func() string {
            if operation != "" {
                return operation
            }
            return eventType
        }(),
        Type:      suitCode,
        Payload:   messageData,
        Key:       code,
        Msg:       msg,
    }

    if err := c.db.Create(&log).Error; err != nil {
        return fmt.Errorf("ошибка сохранения лога: %v", err)
    }

    return nil
}

// Вызывается для обработки сообщений
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	fmt.Printf("Starting to consume messages from partition %d\n", claim.Partition())

	// Используем select для обработки сообщений и контекста
	for {
		select {
		case msg, ok := <-claim.Messages():
			fmt.Printf("Received message from partition %d\n", msg.Partition)

			if !ok {
				return nil // канал закрыт
			}
			
			processed, err := c.isMessageProcessed(msg)
			if err != nil {
				fmt.Printf("Ошибка проверки обработки сообщения: %v\n", err)
				// Логируем ошибку
        if err := c.logToDatabase(msg.Value, fmt.Sprintf("Ошибка проверки обработки сообщения: %v", err), "ERROR"); err != nil {
          fmt.Printf("Ошибка логирования: %v\n", err)
        }
				continue
			}

			if processed {
				fmt.Printf("Сообщение уже обработано: %d/%d\n", msg.Partition, msg.Offset)
				// Логируем пропуск
        if err := c.logToDatabase(msg.Value, "Сообщение уже было обработано", "SKIPPED"); err != nil {
          fmt.Printf("Ошибка логирования: %v\n", err)
        }
				session.MarkMessage(msg, "")
				continue
			}
			
			// Отправляем сырые данные в loader
			if err := c.sendToLoader(session.Context(), msg.Value); err != nil {
				fmt.Printf("Ошибка отправки в loader: %v\n", err)
				// Логируем ошибку
        if logErr := c.logToDatabase(msg.Value, fmt.Sprintf("Ошибка отправки в loader: %v", err), "ERROR"); logErr != nil {
          fmt.Printf("Ошибка логирования: %v\n", logErr)
        }
				continue
			}

			// Сохраняем информацию об обработке в транзакции
			if err := c.markMessageProcessed(msg); err != nil {
				fmt.Printf("Ошибка сохранения состояния: %v\n", err)
				// Логируем ошибку
        if logErr := c.logToDatabase(msg.Value, fmt.Sprintf("Ошибка сохранения состояния: %v", err), "ERROR"); logErr != nil {
          fmt.Printf("Ошибка логирования: %v\n", logErr)
        }
				continue
			}

			// Логируем успешную обработку
      if err := c.logToDatabase(msg.Value, ""); err != nil {
        fmt.Printf("Ошибка логирования: %v\n", err)
      }
			
			fmt.Printf("Сообщение успешно обработано: %d/%d\n", msg.Partition, msg.Offset)
			session.MarkMessage(msg, "")
			session.Commit()

		case <-session.Context().Done():
			return nil // Контекст отменен (например, при ребалансировке)
		}
	}
}

func (c *Consumer) Start() error {
	// Создаем контекст без defer cancel(), так как нам нужно, 
	// чтобы контекст жил все время работы консьюмера
	ctx := context.Background()
	topics := []string{c.topic}

	go func() {
		for {
			if err := c.consumerGroup.Consume(ctx, topics, c); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				fmt.Printf("Error from consumer: %v\n", err)
			}
			
			if ctx.Err() != nil {
				return
			}
			c.ready = make(chan bool)
		}
	}()

	<-c.ready
	fmt.Println("Kafka Consumer started")
	return nil
}