package models

import (
  "github.com/google/uuid"
  "github.com/lib/pq"
)

type LiveStreamMulticast struct {
  Id                      uuid.UUID      `json:"id"`
  LiveStreamKey           uuid.UUID      `json:"live_stream_key_id" gorm:"unique"`
  LiveStreamMulticastUrls pq.StringArray `json:"live_stream_multicast_urls" gorm:"type:text[]"`
  LiveStream              *LiveStreamKey `gorm:"foreignKey:LiveStreamKey;references:StreamKey;constraint:OnDelete:CASCADE,OnUpdate:CASCADE"`
}

type LiveStreamMulticastRepository interface {
  GetLiveStreamMulticastByStreamKey(stream_key uuid.UUID) (*LiveStreamMulticast, error)
}
