package models

import (
	"github.com/google/uuid"
)

type LiveStreamStatistic struct {
	ID                uuid.UUID `json:"id" gorm:"primaryKey;type:uuid"`
	LiveStreamMediaId uuid.UUID `json:"live_stream_media_id" gorm:"UniqueIndex;type:uuid"`
	FpsIn             int16     `json:"fps_in"`
	FpsOut            int16     `json:"fps_out"`
	BitrateIn         float64   `json:"bitrate_in"`
	BitrateOut        float64   `json:"bitrate_out"`
	DataTransferred   float64   `json:"data_transferred"`
}
type LiveStreamStatisticRepository interface {
	UpsertBitrateIn(pathName uuid.UUID, bitrate float64) error
	UpsertBitrateOut(pathName uuid.UUID, bitrate float64) error
	UpsertFPSIn(pathName uuid.UUID, fps int16) error
	UpsertFPSOut(pathName uuid.UUID, fps int16) error
	UpsertDataTransferred(pathname uuid.UUID, dataTransferred float64) error
}
