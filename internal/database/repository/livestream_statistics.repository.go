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

func (l *LiveStreamStatisticRepository) UpsertBitrateIn(streamKey uuid.UUID, bitrate float64) error {
	record := models.LiveStreamStatistic{
		ID:            uuid.New(),
		LiveStreamKey: streamKey,
		BitrateIn:     bitrate,
	}

	result := l.db.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_key"}},
		DoUpdates: clause.AssignmentColumns([]string{"bitrate_in"}),
	}).Create(&record)

	return result.Error
}
func (l *LiveStreamStatisticRepository) UpsertBitrateOut(streamKey uuid.UUID, bitrate float64) error {
	record := models.LiveStreamStatistic{
		ID:            uuid.New(),
		LiveStreamKey: streamKey,
		BitrateOut:    bitrate,
	}

	result := l.db.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_key"}},
		DoUpdates: clause.AssignmentColumns([]string{"bitrate_out"}),
	}).Create(&record)

	return result.Error
}

func (l *LiveStreamStatisticRepository) UpsertFPSIn(streamKey uuid.UUID, fps int16) error {
	record := models.LiveStreamStatistic{
		ID:            uuid.New(),
		LiveStreamKey: streamKey,
		FpsIn:         fps,
	}

	result := l.db.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_key"}},
		DoUpdates: clause.AssignmentColumns([]string{"fps_in"}),
	}).Create(&record)

	return result.Error
}

func (l *LiveStreamStatisticRepository) UpsertFPSOut(streamKey uuid.UUID, fps int16) error {
	record := models.LiveStreamStatistic{
		ID:            uuid.New(),
		LiveStreamKey: streamKey,
		FpsOut:        fps,
	}

	result := l.db.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_key"}},
		DoUpdates: clause.AssignmentColumns([]string{"fps_out"}),
	}).Create(&record)

	return result.Error
}

func (l *LiveStreamStatisticRepository) UpsertNumberOfRequests(streamKey uuid.UUID, numberOfRequests int) error {
	record := models.LiveStreamStatistic{
		ID:               uuid.New(),
		LiveStreamKey:    streamKey,
		NumberOfRequests: numberOfRequests,
	}

	result := l.db.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_key"}},
		DoUpdates: clause.AssignmentColumns([]string{"number_of_requests"}),
	}).Create(&record)

	return result.Error
}

func (l *LiveStreamStatisticRepository) UpsertDataTransferred(streamKey uuid.UUID, dataTransferred float64) error {
	record := models.LiveStreamStatistic{
		ID:              uuid.New(),
		LiveStreamKey:   streamKey,
		DataTransferred: dataTransferred,
	}

	result := l.db.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "live_stream_key"}},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"data_transferred": gorm.Expr("live_stream_statistics.data_transferred + ?", dataTransferred),
		}),
	}).Create(&record)

	return result.Error
}

func (l *LiveStreamStatisticRepository) UpsertDevice(streamKey uuid.UUID, device string) error {
	record := models.LiveStreamStatistic{
		ID:            uuid.New(),
		LiveStreamKey: streamKey,
		Device:        device,
	}

	result := l.db.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_key"}},
		DoUpdates: clause.AssignmentColumns([]string{"device"}),
	}).Create(&record)

	return result.Error
}

func (l *LiveStreamStatisticRepository) UpsertOS(streamKey uuid.UUID, os string) error {
	record := models.LiveStreamStatistic{
		ID:            uuid.New(),
		LiveStreamKey: streamKey,
		OS:            os,
	}

	result := l.db.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_key"}},
		DoUpdates: clause.AssignmentColumns([]string{"os"}),
	}).Create(&record)

	return result.Error
}

func (l *LiveStreamStatisticRepository) UpsertLocation(streamKey uuid.UUID, location string) error {
	record := models.LiveStreamStatistic{
		ID:            uuid.New(),
		LiveStreamKey: streamKey,
		Location:      location,
	}

	result := l.db.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_key"}},
		DoUpdates: clause.AssignmentColumns([]string{"location"}),
	}).Create(&record)

	return result.Error
}

func NewLiveStreamStatisticsRepository(db *gorm.DB) models.LiveStreamStatisticRepository {
	return &LiveStreamStatisticRepository{db: db}
}
