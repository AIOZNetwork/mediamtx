package repository

import (
  "github.com/bluenviron/mediamtx/internal/database"
  "github.com/bluenviron/mediamtx/internal/models"
  "github.com/google/uuid"
)

type liveStreamMulticastRepository struct{}

func (l *liveStreamMulticastRepository) GetLiveStreamMulticastByStreamKey(stream_key uuid.UUID) (*models.LiveStreamMulticast, error) {
  var liveStreamMulticast models.LiveStreamMulticast
  if err := database.DB.Table("live_stream_multicasts").Where("live_stream_key = ?", stream_key).First(&liveStreamMulticast).Error; err != nil {
    return nil, err
  }
  return &liveStreamMulticast, nil
}

func NewLiveStreamMulticastRepository() models.LiveStreamMulticastRepository {
  return &liveStreamMulticastRepository{}
}
