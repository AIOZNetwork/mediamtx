package repository

import (
	"github.com/bluenviron/mediamtx/internal/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type liveStreamKeyRepository struct {
	db *gorm.DB
}

func (l *liveStreamKeyRepository) GetLiveStreamKeyByStreamKey(streamKey uuid.UUID) (*models.LiveStreamKey, error) {
	var liveStreamKey models.LiveStreamKey
	if err := l.db.Table("live_stream_keys").Where("stream_key = ?", streamKey).First(&liveStreamKey).Error; err != nil {
		return nil, err
	}
	return &liveStreamKey, nil
}

func NewLiveStreamKeyRepository(db *gorm.DB) models.LiveStreamKeyRepository {
	return &liveStreamKeyRepository{
		db: db,
	}
}