package config

import (
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
    DBHost     string
    DBPort     string
    DBUser     string
    DBPassword string
    DBName     string
    KafkaBrokers string
    KafkaTopic   string
    Port         string
    ServiceConsumerURL   string
    KafkaGroupID string
}

func LoadConfig() (*Config, error) {
    if err := godotenv.Load(); err != nil {
        return nil, err
    }

    return &Config{
        DBHost:       os.Getenv("DB_HOST"),
        DBPort:       os.Getenv("DB_PORT"),
        DBUser:       os.Getenv("DB_USER"),
        DBPassword:   os.Getenv("DB_PASSWORD"),
        DBName:       os.Getenv("DB_NAME"),
        KafkaBrokers: os.Getenv("KAFKA_BROKERS"),
        KafkaTopic:   os.Getenv("KAFKA_TOPIC"),
        Port:         os.Getenv("PORT"),
        ServiceConsumerURL:   os.Getenv("SERVICE_CONSUMER_URL"),
        KafkaGroupID: os.Getenv("KAFKA_GROUP_ID"),
    }, nil
}