package models

import (
	"context"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

const (
	LiveHLSSegmentStatusLocal        = "local_only"
	LiveHLSSegmentStatusUploading    = "uploading"
	LiveHLSSegmentStatusUploadedS3   = "uploaded_s3"
	LiveHLSSegmentStatusUploadFailed = "upload_failed"
)

type LiveHLSSegment struct {
	ID              uuid.UUID      `json:"id" gorm:"primaryKey;type:uuid"`
	StreamID        string         `json:"stream_id" gorm:"uniqueIndex:idx_live_hls_segment_stream_name"`
	BaseStreamID    string         `json:"base_stream_id" gorm:"index"`
	SessionID       string         `json:"session_id" gorm:"index"`
	MuxSessionID    string         `json:"mux_session_id" gorm:"index"`
	TrackType       string         `json:"track_type" gorm:"index"`
	Rendition       string         `json:"rendition" gorm:"index"`
	SegmentName     string         `json:"segment_name" gorm:"uniqueIndex:idx_live_hls_segment_stream_name"`
	InitSegmentName string         `json:"init_segment_name"`
	InitStorageKey  string         `json:"init_storage_key"`
	PlaylistName    string         `json:"playlist_name"`
	LocalPath       string         `json:"local_path"`
	StorageBackend  string         `json:"storage_backend"`
	StorageKey      string         `json:"storage_key"`
	StorageETag     string         `json:"storage_etag"`
	Status          string         `json:"status"`
	StartedAt       time.Time      `json:"started_at" gorm:"index"`
	DurationMS      int64          `json:"duration_ms" gorm:"default:0"`
	SizeBytes       int64          `json:"size_bytes"`
	Sequence        *int64         `json:"sequence"`
	MediaSequence   *int64         `json:"media_sequence" gorm:"index"`
	ContentType     string         `json:"content_type"`
	ReadyMS         *int64         `json:"ready_ms,omitempty" gorm:"column:ready_ms"`
	LocalCount      int64          `json:"local_count" gorm:"column:local_count;default:0"`
	RedirectCount   int64          `json:"redirect_count" gorm:"column:redirect_count;default:0"`
	ProxyCount      int64          `json:"proxy_count" gorm:"column:proxy_count;default:0"`
	MissCount       int64          `json:"miss_count" gorm:"column:miss_count;default:0"`
	UploadedAt      *time.Time     `json:"uploaded_at"`
	LocalDeletedAt  *time.Time     `json:"local_deleted_at"`
	CreatedAt       time.Time      `json:"created_at"`
	UpdatedAt       time.Time      `json:"updated_at"`
	DeletedAt       gorm.DeletedAt `json:"-" gorm:"index"`
}

func (LiveHLSSegment) TableName() string {
	return "live_hls_segments"
}

type LiveHLSSegmentRepository interface {
	UpsertLocal(segment LiveHLSSegment) error
	UpsertUploading(segment LiveHLSSegment) error
	UpsertUploaded(segment LiveHLSSegment) error
	UpsertUploadFailed(segment LiveHLSSegment) error
	MarkLocalDeleted(streamID, segmentName string, deletedAt time.Time) error
	GetByStorageKey(storageKey string) (*LiveHLSSegment, error)
	GetByStreamAndSegment(streamID, segmentName string) (*LiveHLSSegment, error)
	ListWindow(streamID, sessionID string, since time.Time) ([]LiveHLSSegment, error)
	ListFMP4Window(streamID string, since time.Time) ([]LiveHLSSegment, error)
	MaxSequence(streamID string) (int64, bool, error)
	// ListUploadedObjects returns the rows of streamID uploaded to backend under
	// prefix (storage_key = prefix/...) with their storage_e_tag, soft-deleted
	// rows included: the remote object exists until the provider deletes it.
	ListUploadedObjects(backend, streamID, prefix string) ([]LiveHLSSegment, error)
	SetReadyMS(ctx context.Context, streamID, segmentName string, ms int64) error
}

// LiveHLSSegmentServeCounter applies all pending outcome deltas in one update.
type LiveHLSSegmentServeCounter interface {
	AddServeCounts(ctx context.Context, streamID, segmentName string, local, redirect, proxy, miss int64) error
}
