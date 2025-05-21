package models

import "github.com/google/uuid"

type LiveStreamMedia struct {
	Id              uuid.UUID `json:"id"`
	LiveStreamKeyId uuid.UUID `json:"live_stream_key_id"`
	Status          string    `json:"status"`
	ConnectionId    string    `json:"connection_id"`
}

type LiveStreamMediaRepository interface {
	GetStreamMediaByConnId(connId uuid.UUID) (*LiveStreamMedia, error)
	GetStreamMediaAvaialbleByStreamKey(streamKey uuid.UUID) (*LiveStreamMedia, error)
	GetStreamKeyExist(streamKey uuid.UUID) uuid.UUID
	GetStreamKeyByStreamID(streamID uuid.UUID) (uuid.UUID, error)
	UpsertStreamMedia(streamKey uuid.UUID, streamID uuid.UUID) error
}
