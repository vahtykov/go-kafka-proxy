package config

import (
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
    KafkaSSL   string
    AppMode    string
    DBHost     string
    DBPort     string
    DBUser     string
    DBPassword string
    DBName     string
    KafkaBrokers string
    KafkaTopic   string
    Port         string
    ServiceLoaderURL   string
    KafkaGroupID string
    KafkaCertDir string
}

func LoadConfig() (*Config, error) {
    if err := godotenv.Load(); err != nil {
        return nil, err
    }

    cfg := &Config{
        KafkaSSL:     os.Getenv("KAFKA_SSL"),
        AppMode:      os.Getenv("APP_MODE"),
        DBHost:       os.Getenv("DB_HOST"),
        DBPort:       os.Getenv("DB_PORT"),
        DBUser:       os.Getenv("DB_USER"),
        DBPassword:   os.Getenv("DB_PASSWORD"),
        DBName:       os.Getenv("DB_NAME"),
        KafkaBrokers: os.Getenv("KAFKA_BROKERS"),
        KafkaTopic:   os.Getenv("KAFKA_TOPIC"),
        Port:         os.Getenv("PORT"),
        ServiceLoaderURL:   os.Getenv("SERVICE_LOADER_URL"),
        KafkaGroupID: os.Getenv("KAFKA_GROUP_ID"),
    }

    if cfg.KafkaSSL == "Y" {
        if cfg.AppMode == "local" {
            cfg.KafkaCertDir = "conf/secrets/kafka"
        } else {
            cfg.KafkaCertDir = "/vault/secrets/kafka"
        }
    }

    return cfg, nil
}