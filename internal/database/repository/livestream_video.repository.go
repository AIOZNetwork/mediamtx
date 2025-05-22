package repository

import (
	"github.com/bluenviron/mediamtx/internal/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type LiveStreamVideoRepository struct {
	db *gorm.DB
}

func NewLiveStreamVideoRepository(db *gorm.DB) *LiveStreamVideoRepository {
	return &LiveStreamVideoRepository{db: db}
}

func (l *LiveStreamVideoRepository) GetStreamMediaByConnId(connId uuid.UUID) (*models.LiveStreamMedia, error) {
	var video models.LiveStreamMedia
	err := l.db.Table("live_stream_media").Where("live_stream_media.connection_id = ?", connId).First(&video).Error
	if err != nil {
		return nil, err
	}
	return &video, nil
}

func (l *LiveStreamVideoRepository) GetStreamMediaAvaialbleByStreamKey(streamKey uuid.UUID) (*models.LiveStreamMedia, error) {
	var video models.LiveStreamMedia
	err := l.db.Table("live_stream_media").
		Select("live_stream_media.id, live_stream_media.live_stream_key_id, live_stream_media.status").
		Joins("JOIN live_stream_keys ON live_stream_keys.id = live_stream_media.live_stream_key_id").
		Where("live_stream_keys.stream_key = ? AND (live_stream_media.status = ? OR live_stream_media.status = ?)", streamKey, "streaming", "created").First(&video).Error
	if err != nil {
		return nil, err
	}
	return &video, nil
}

func (l *LiveStreamVideoRepository) GetStreamKeyExist(streamKey uuid.UUID) uuid.UUID {
	var result string
	l.db.Table("live_stream_keys").
		Select("live_stream_keys.stream_key").
		Where("live_stream_keys.stream_key = ?", streamKey).Scan(&result)
	uuidKey, err := uuid.Parse(result)
	if err != nil {
		return uuid.Nil
	}

	return uuidKey
}

func (l *LiveStreamVideoRepository) GetStreamKeyByStreamID(streamID uuid.UUID) (uuid.UUID, error) {
	var streamKey string
	err := l.db.Table("live_stream_media").
		Select("live_stream_keys.stream_key").
		Joins("JOIN live_stream_keys ON live_stream_keys.id = live_stream_media.live_stream_key_id").
		Where("live_stream_media.id = ?", streamID).Scan(&streamKey).Error
	if err != nil {
		return uuid.Nil, err
	}

	uuidStreamKey, err := uuid.Parse(streamKey)
	if err != nil {
		return uuid.Nil, err
	}
	return uuidStreamKey, nil
}

func (l *LiveStreamVideoRepository) UpsertStreamMedia(streamKey uuid.UUID, streamID uuid.UUID) error {

	var streamKeyId string
	err := l.db.Table("live_stream_keys").
		Select("live_stream_keys.id").
		Where("live_stream_keys.stream_key = ?", streamKey).Scan(&streamKeyId).Error

	if err != nil || streamKeyId == uuid.Nil.String() {
		return err
	}

	uuidStreamKeyId, err := uuid.Parse(streamKeyId)
	if err != nil {
		return err
	}

	livestreamVideo := &models.LiveStreamMedia{
		Id:              streamID,
		LiveStreamKeyId: uuidStreamKeyId,
	}

	err = l.db.Table("live_stream_media").Create(&livestreamVideo).Error
	if err != nil {
		return err
	}

	return nil
}
