package models

import (
	"time"

	"github.com/google/uuid"
)

type TrackLoadLog struct {
    ID        int       `gorm:"primarykey;column:id"`
    UUID      uuid.UUID `gorm:"type:uuid;column:uuid;not null"`
    Operation string    `gorm:"column:operation;not null"`
    Type      string    `gorm:"column:type;not null"`
    Payload   []byte    `gorm:"type:jsonb;column:payload"`
    Key       string    `gorm:"column:key"`
    Msg       string    `gorm:"column:msg"`
    CreatedAt time.Time `gorm:"column:createdAt;default:CURRENT_TIMESTAMP"`
    UpdatedAt time.Time `gorm:"column:updatedAt;default:CURRENT_TIMESTAMP"`
}

func (TrackLoadLog) TableName() string {
    return "track_load_log"
}