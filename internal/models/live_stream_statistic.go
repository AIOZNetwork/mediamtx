package models

import (
	"github.com/google/uuid"
)

type LiveStreamStatistic struct {
	ID               uuid.UUID      `json:"id" gorm:"primaryKey"`
	LiveStreamKey    uuid.UUID      `json:"live_stream_key" gorm:"uniqueIndex"`
	FpsIn            int16          `json:"fps_in"`
	FpsOut           int16          `json:"fps_out"`
	BitrateIn        float64        `json:"bitrate_in"`
	BitrateOut       float64        `json:"bitrate_out"`
	NumberOfRequests int            `json:"number_of_requests"`
	DataTransferred  float64        `json:"data_transferred"`
	Device           string         `json:"device"`
	OS               string         `json:"os"`
	Location         string         `json:"location"`
	LiveStream       *LiveStreamKey `gorm:"foreignKey:LiveStreamKey;references:StreamKey;constraint:OnDelete:CASCADE,OnUpdate:CASCADE"`
}
type LiveStreamStatisticRepository interface {
	UpsertBitrateIn(stream_key uuid.UUID, bitrate float64) error
	UpsertBitrateOut(stream_key uuid.UUID, bitrate float64) error
	UpsertFPSIn(stream_key uuid.UUID, fps int16) error
	UpsertFPSOut(stream_key uuid.UUID, fps int16) error
	UpsertNumberOfRequests(stream_key uuid.UUID, number_of_requests int) error
	UpsertDataTransferred(stream_key uuid.UUID, data_transferred float64) error
	UpsertDevice(stream_key uuid.UUID, device string) error
	UpsertOS(stream_key uuid.UUID, os string) error
	UpsertLocation(stream_key uuid.UUID, location string) error
}
