package models

import (
	"github.com/google/uuid"
	"github.com/lib/pq"
)

type LiveStreamMulticast struct {
  Id                        uuid.UUID
  LiveStreamKey             uuid.UUID
  LiveStreamMulticastUrls   pq.StringArray `json:"live_stream_multicast_urls" gorm:"type:text[]"`
}

type LiveStreamMulticastRepository interface {
  GetLiveStreamMulticastByStreamKey(stream_key uuid.UUID) (*LiveStreamMulticast, error)
}