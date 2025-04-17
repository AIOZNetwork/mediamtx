package models

import "github.com/google/uuid"

type LiveStreamVideo struct {
	Id              uuid.UUID `json:"id"`
	LiveStreamKeyId uuid.UUID `json:"live_stream_key_id"`
	Status          string    `json:"status"`
}

type LiveStreamVideoRepository interface {
	GetStreamVideoAvaialbleByStreamKey(streamKey uuid.UUID) (*LiveStreamVideo, error)
	GetStreamKeyExist(streamKey uuid.UUID) uuid.UUID
	GetStreamKeyByStreamID(streamID uuid.UUID) (uuid.UUID, error)
	UpsertStreamVideo(streamKey uuid.UUID, streamID uuid.UUID) error
}
