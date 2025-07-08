package models

import (
	"github.com/google/uuid"
	"github.com/lib/pq"
)

type LiveStreamMulticast struct {
	Id                      uuid.UUID      `json:"id"`
	LiveStreamKeyId         uuid.UUID      `json:"live_stream_key_id" gorm:"unique;type:uuid"`
	LiveStreamMulticastUrls pq.StringArray `json:"live_stream_multicast_urls" gorm:"type:text[]"`
	LiveStreamKey           *LiveStreamKey `json:"-"`
}

type LiveStreamMulticastRepository interface {
	GetLiveStreamMulticastByStreamKey(stream_key uuid.UUID) (*LiveStreamMulticast, error)
}
