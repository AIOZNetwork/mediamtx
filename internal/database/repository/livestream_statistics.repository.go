package repository

import (
	"github.com/bluenviron/mediamtx/internal/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type LiveStreamStatisticRepository struct {
	db *gorm.DB
}

func (l *LiveStreamStatisticRepository) UpsertBitrateIn(pathName uuid.UUID, bitrate float64) error {
	record := models.LiveStreamStatistic{
		ID:                uuid.New(),
		LiveStreamMediaId: pathName,
		BitrateIn:         bitrate,
	}

	result := l.db.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_media_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"bitrate_in"}),
	}).Create(&record)

	return result.Error
}
func (l *LiveStreamStatisticRepository) UpsertBitrateOut(pathName uuid.UUID, bitrate float64) error {
	record := models.LiveStreamStatistic{
		ID:                uuid.New(),
		LiveStreamMediaId: pathName,
		BitrateOut:        bitrate,
	}

	result := l.db.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_media_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"bitrate_out"}),
	}).Create(&record)

	return result.Error
}

func (l *LiveStreamStatisticRepository) UpsertFPSIn(pathName uuid.UUID, fps int16) error {
	record := models.LiveStreamStatistic{
		ID:                uuid.New(),
		LiveStreamMediaId: pathName,
		FpsIn:             fps,
	}

	result := l.db.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_media_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"fps_in"}),
	}).Create(&record)

	return result.Error
}

func (l *LiveStreamStatisticRepository) UpsertFPSOut(pathName uuid.UUID, fps int16) error {
	record := models.LiveStreamStatistic{
		ID:                uuid.New(),
		LiveStreamMediaId: pathName,
		FpsOut:            fps,
	}

	result := l.db.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_media_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"fps_out"}),
	}).Create(&record)

	return result.Error
}

func (l *LiveStreamStatisticRepository) UpsertNumberOfRequests(pathName uuid.UUID, numberOfRequests int) error {
	record := models.LiveStreamStatistic{
		ID:                uuid.New(),
		LiveStreamMediaId: pathName,
		NumberOfRequests:  numberOfRequests,
	}

	result := l.db.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_media_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"number_of_requests"}),
	}).Create(&record)

	return result.Error
}

func (l *LiveStreamStatisticRepository) UpsertDataTransferred(pathName uuid.UUID, dataTransferred float64) error {
	record := models.LiveStreamStatistic{
		ID:                uuid.New(),
		LiveStreamMediaId: pathName,
		DataTransferred:   dataTransferred,
	}

	result := l.db.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "live_stream_media_id"}},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"data_transferred": gorm.Expr("live_stream_statistics.data_transferred + ?", dataTransferred),
		}),
	}).Create(&record)

	return result.Error
}

func NewLiveStreamStatisticsRepository(db *gorm.DB) models.LiveStreamStatisticRepository {
	return &LiveStreamStatisticRepository{db: db}
}
