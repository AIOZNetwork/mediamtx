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
		LiveStreamVideoId: pathName,
		BitrateIn:         bitrate,
	}

	result := l.db.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_video_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"bitrate_in"}),
	}).Create(&record)

	return result.Error
}
func (l *LiveStreamStatisticRepository) UpsertBitrateOut(pathName uuid.UUID, bitrate float64) error {
	record := models.LiveStreamStatistic{
		ID:                uuid.New(),
		LiveStreamVideoId: pathName,
		BitrateOut:        bitrate,
	}

	result := l.db.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_video_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"bitrate_out"}),
	}).Create(&record)

	return result.Error
}

func (l *LiveStreamStatisticRepository) UpsertFPSIn(pathName uuid.UUID, fps int16) error {
	record := models.LiveStreamStatistic{
		ID:                uuid.New(),
		LiveStreamVideoId: pathName,
		FpsIn:             fps,
	}

	result := l.db.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_video_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"fps_in"}),
	}).Create(&record)

	return result.Error
}

func (l *LiveStreamStatisticRepository) UpsertFPSOut(pathName uuid.UUID, fps int16) error {
	record := models.LiveStreamStatistic{
		ID:                uuid.New(),
		LiveStreamVideoId: pathName,
		FpsOut:            fps,
	}

	result := l.db.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_video_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"fps_out"}),
	}).Create(&record)

	return result.Error
}

func (l *LiveStreamStatisticRepository) UpsertNumberOfRequests(pathName uuid.UUID, numberOfRequests int) error {
	record := models.LiveStreamStatistic{
		ID:                uuid.New(),
		LiveStreamVideoId: pathName,
		NumberOfRequests:  numberOfRequests,
	}

	result := l.db.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_video_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"number_of_requests"}),
	}).Create(&record)

	return result.Error
}

func (l *LiveStreamStatisticRepository) UpsertDataTransferred(pathName uuid.UUID, dataTransferred float64) error {
	record := models.LiveStreamStatistic{
		ID:                uuid.New(),
		LiveStreamVideoId: pathName,
		DataTransferred:   dataTransferred,
	}

	result := l.db.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "live_stream_video_id"}},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"data_transferred": gorm.Expr("live_stream_statistics.data_transferred + ?", dataTransferred),
		}),
	}).Create(&record)

	return result.Error
}

func (l *LiveStreamStatisticRepository) UpsertDevice(pathName uuid.UUID, device string) error {
	record := models.LiveStreamStatistic{
		ID:                uuid.New(),
		LiveStreamVideoId: pathName,
		Device:            device,
	}

	result := l.db.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_video_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"device"}),
	}).Create(&record)

	return result.Error
}

func (l *LiveStreamStatisticRepository) UpsertOS(pathName uuid.UUID, os string) error {
	record := models.LiveStreamStatistic{
		ID:                uuid.New(),
		LiveStreamVideoId: pathName,
		OS:                os,
	}

	result := l.db.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_video_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"os"}),
	}).Create(&record)

	return result.Error
}

func (l *LiveStreamStatisticRepository) UpsertLocation(pathName uuid.UUID, location string) error {
	record := models.LiveStreamStatistic{
		ID:                uuid.New(),
		LiveStreamVideoId: pathName,
		Location:          location,
	}

	result := l.db.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_video_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"location"}),
	}).Create(&record)

	return result.Error
}

func NewLiveStreamStatisticsRepository(db *gorm.DB) models.LiveStreamStatisticRepository {
	return &LiveStreamStatisticRepository{db: db}
}
