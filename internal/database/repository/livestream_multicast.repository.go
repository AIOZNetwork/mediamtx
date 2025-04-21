package repository

import (
	"github.com/bluenviron/mediamtx/internal/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type liveStreamMulticastRepository struct {
	db *gorm.DB
}

func (l *liveStreamMulticastRepository) GetLiveStreamMulticastByStreamKey( stream_key uuid.UUID) (*models.LiveStreamMulticast, error) {
	var liveStreamMulticast models.LiveStreamMulticast
	if err := l.db.Table("live_stream_multicasts").Where("live_stream_key = ?", stream_key).First(&liveStreamMulticast).Error; err != nil {
		return nil, err
	}
	return &liveStreamMulticast, nil
}

func NewLiveStreamMulticastRepository(db *gorm.DB) models.LiveStreamMulticastRepository {
	return &liveStreamMulticastRepository{
		db: db,
	}
}
