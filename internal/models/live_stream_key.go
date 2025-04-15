package models

import (
	"time"

	"github.com/google/uuid"
)

type LiveStreamKey struct {
	Id                 uuid.UUID          `json:"id"`
	UserId             uuid.UUID          `json:"user_id"`
	Name               string             `json:"name"`
	Save               bool               `json:"save"`
	StreamKey          uuid.UUID          `json:"stream_key" gorm:"unique"`
	CreatedAt          time.Time          `json:"created_at"`
	UpdatedAt          time.Time          `json:"updated_at"`
	TotalSaveVideo     int64              `json:"-" gorm:"-"`
	TotalLiveStreaming int64              `json:"-" gorm:"-"`
}