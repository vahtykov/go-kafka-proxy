package models

import "time"

type ProcessedMessage struct {
    ID          uint      `gorm:"primarykey;column:id"`
    MessageID   string    `gorm:"column:message_id;not null"`
    Topic       string    `gorm:"column:topic;not null"`
    Partition   int32     `gorm:"column:partition;not null"`
    Offset      int64     `gorm:"column:offset;not null"`
    ProcessedAt time.Time `gorm:"column:processed_at;not null"`
}

type KafkaOffset struct {
    ID        uint      `gorm:"primarykey;column:id"`
    Topic     string    `gorm:"column:topic;not null"`
    Partition int32     `gorm:"column:partition;not null"`
    Offset    int64     `gorm:"column:offset;not null"`
    GroupID   string    `gorm:"column:group_id;not null"`
    UpdatedAt time.Time `gorm:"column:updated_at;not null"`
}