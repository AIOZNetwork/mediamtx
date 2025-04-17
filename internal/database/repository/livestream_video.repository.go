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

func (l *LiveStreamVideoRepository) GetStreamVideoStreamingByStreamKey(streamKey uuid.UUID) (*models.LiveStreamVideo, error) {
	var video models.LiveStreamVideo
	err := l.db.Table("live_stream_videos").
		Select("live_stream_videos.id, live_stream_videos.live_stream_key_id").
		Joins("JOIN live_stream_keys ON live_stream_keys.id = live_stream_videos.live_stream_key_id").
		Where("live_stream_keys.stream_key = ? AND live_stream_videos.status = ?", streamKey, "streaming").First(&video).Error
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
	err := l.db.Table("live_stream_videos").
		Select("live_stream_keys.stream_key").
		Joins("JOIN live_stream_keys ON live_stream_keys.id = live_stream_videos.live_stream_key_id").
		Where("live_stream_videos.id = ?", streamID).Scan(&streamKey).Error
	if err != nil {
		return uuid.Nil, err
	}

	uuidStreamKey, err := uuid.Parse(streamKey)
	if err != nil {
		return uuid.Nil, err
	}
	return uuidStreamKey, nil
}

func (l *LiveStreamVideoRepository) UpsertStreamVideo(streamKey uuid.UUID, streamID uuid.UUID) error {

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

	livestreamVideo := &models.LiveStreamVideo{
		Id:              streamID,
		LiveStreamKeyId: uuidStreamKeyId,
	}

	err = l.db.Table("live_stream_videos").Create(&livestreamVideo).Error
	if err != nil {
		return err
	}

	return nil
}
