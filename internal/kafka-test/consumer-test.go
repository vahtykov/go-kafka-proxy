package main

import (
	"context"
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

func main() {
    config := sarama.NewConfig()
    config.Consumer.Return.Errors = true
    
    // Создаем клиента
    brokers := []string{"localhost:9092"}
    client, err := sarama.NewClient(brokers, config)
    if err != nil {
        log.Fatalf("Error creating client: %v", err)
    }
    defer client.Close()

    // Создаем consumer
    consumer, err := sarama.NewConsumerFromClient(client)
    if err != nil {
        log.Fatalf("Error creating consumer: %v", err)
    }
    defer consumer.Close()

    // Указываем топик и партицию
    topic := "plans"
    partition := int32(0)
    
    // Получаем партицию
    partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
    if err != nil {
        log.Fatalf("Error creating partition consumer: %v", err)
    }
    defer partitionConsumer.Close()

    // Создаем контекст с отменой
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Читаем сообщения
    for {
        select {
        case msg := <-partitionConsumer.Messages():
            fmt.Printf("Message topic:%s partition:%d offset:%d\n value:%s\n",
                msg.Topic, msg.Partition, msg.Offset, string(msg.Value))

        case err := <-partitionConsumer.Errors():
            fmt.Printf("Error: %v\n", err)

        case <-ctx.Done():
            return
        }
    }
}