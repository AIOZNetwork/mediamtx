package repository

import (
	"github.com/bluenviron/mediamtx/internal/database"
	"github.com/bluenviron/mediamtx/internal/models"
	"github.com/google/uuid"
	"gorm.io/gorm/clause"
)

type LiveStreamStatisticRepository struct{}

func (l *LiveStreamStatisticRepository) UpsertBitrateIn(streamKey uuid.UUID, bitrate float64) error {
	record := models.LiveStreamStatistic{
		ID:            uuid.New(),
		LiveStreamKey: streamKey,
		Bitrate_in:    bitrate,
	}

	result := database.DB.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_key"}},
		DoUpdates: clause.AssignmentColumns([]string{"bitrate_in"}),
	}).Create(&record)

	return result.Error
}
func (l *LiveStreamStatisticRepository) UpsertBitrateOut(streamKey uuid.UUID, bitrate float64) error {
	record := models.LiveStreamStatistic{
		ID:            uuid.New(),
		LiveStreamKey: streamKey,
		Bitrate_out:   bitrate,
	}

	result := database.DB.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_key"}},
		DoUpdates: clause.AssignmentColumns([]string{"bitrate_out"}),
	}).Create(&record)

	return result.Error
}

func (l *LiveStreamStatisticRepository) UpsertFPSIn(streamKey uuid.UUID, fps int16) error {
	record := models.LiveStreamStatistic{
		ID:            uuid.New(),
		LiveStreamKey: streamKey,
		FPS_in:        fps,
	}

	result := database.DB.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_key"}},
		DoUpdates: clause.AssignmentColumns([]string{"fps_in"}),
	}).Create(&record)

	return result.Error
}

func (l *LiveStreamStatisticRepository) UpsertFPSOut(streamKey uuid.UUID, fps int16) error {
	record := models.LiveStreamStatistic{
		ID:            uuid.New(),
		LiveStreamKey: streamKey,
		FPS_out:       fps,
	}

	result := database.DB.Table("live_stream_statistics").Clauses(clause.OnConflict{
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

	result := database.DB.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_key"}},
		DoUpdates: clause.AssignmentColumns([]string{"number_of_requests"}),
	}).Create(&record)

	return result.Error
}

func (l *LiveStreamStatisticRepository) UpsertDataTransferred(streamKey uuid.UUID, dataTransferred int) error {
	record := models.LiveStreamStatistic{
		ID:             uuid.New(),
		LiveStreamKey:  streamKey,
		DataTransfered: dataTransferred,
	}

	result := database.DB.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_key"}},
		DoUpdates: clause.AssignmentColumns([]string{"data_transferred"}),
	}).Create(&record)

	return result.Error
}

func (l *LiveStreamStatisticRepository) UpsertDevice(streamKey uuid.UUID, device string) error {
	record := models.LiveStreamStatistic{
		ID:            uuid.New(),
		LiveStreamKey: streamKey,
		Device:        device,
	}

	result := database.DB.Table("live_stream_statistics").Clauses(clause.OnConflict{
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

	result := database.DB.Table("live_stream_statistics").Clauses(clause.OnConflict{
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

	result := database.DB.Table("live_stream_statistics").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "live_stream_key"}},
		DoUpdates: clause.AssignmentColumns([]string{"location"}),
	}).Create(&record)

	return result.Error
}

func NewLiveStreamStatisticsRepository() models.LiveStreamStatisticRepository {
	return &LiveStreamStatisticRepository{}
}
