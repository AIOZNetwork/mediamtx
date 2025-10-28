package models

import (
	"time"

	"github.com/google/uuid"
)

type LiveStreamKey struct {
	Id                 uuid.UUID `json:"id" gorm:"type:uuid;primaryKey"`
	UserId             uuid.UUID `json:"user_id"`
	Name               string    `json:"name"`
	Save               bool      `json:"save"`
	Type               string    `json:"type"`
	StreamKey          uuid.UUID `json:"stream_key" gorm:"unique;type:uuid"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
	TotalSaveVideo     int64     `json:"-" gorm:"-"`
	TotalLiveStreaming int64     `json:"-" gorm:"-"`
}

type LiveStreamKeyRepository interface {
	GetLiveStreamKeyByStreamKey(streamKey uuid.UUID) (*LiveStreamKey, error)
}
