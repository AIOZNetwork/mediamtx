package models

import (
	"github.com/google/uuid"
)

type LiveStreamStatistic struct {
	ID                uuid.UUID        `json:"id" gorm:"primaryKey"`
	LiveStreamVideoId uuid.UUID        `json:"live_stream_video_id" gorm:"UniqueIndex"`
	FpsIn             int16            `json:"fps_in"`
	FpsOut            int16            `json:"fps_out"`
	BitrateIn         float64          `json:"bitrate_in"`
	BitrateOut        float64          `json:"bitrate_out"`
	NumberOfRequests  int              `json:"number_of_requests"`
	DataTransferred   float64          `json:"data_transferred"`
	Device            string           `json:"device"`
	OS                string           `json:"os"`
	Location          string           `json:"location"`
}
type LiveStreamStatisticRepository interface {
	UpsertBitrateIn(pathName uuid.UUID, bitrate float64) error
	UpsertBitrateOut(pathName uuid.UUID, bitrate float64) error
	UpsertFPSIn(pathName uuid.UUID, fps int16) error
	UpsertFPSOut(pathName uuid.UUID, fps int16) error
	UpsertNumberOfRequests(pathName uuid.UUID, numberOfRequests int) error
	UpsertDataTransferred(pathname uuid.UUID, dataTransferred float64) error
	UpsertDevice(pathName uuid.UUID, device string) error
	UpsertOS(pathName uuid.UUID, os string) error
	UpsertLocation(pathName uuid.UUID, location string) error
}
