package models

import (
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
	ID             uuid.UUID      `json:"id" gorm:"primaryKey;type:uuid"`
	StreamID       string         `json:"stream_id" gorm:"uniqueIndex:idx_live_hls_segment_stream_name"`
	SegmentName    string         `json:"segment_name" gorm:"uniqueIndex:idx_live_hls_segment_stream_name"`
	LocalPath      string         `json:"local_path"`
	StorageBackend string         `json:"storage_backend"`
	StorageKey     string         `json:"storage_key"`
	StorageETag    string         `json:"storage_etag"`
	Status         string         `json:"status"`
	DurationMS     int64          `json:"duration_ms" gorm:"default:0"`
	SizeBytes      int64          `json:"size_bytes"`
	Sequence       *int64         `json:"sequence"`
	ContentType    string         `json:"content_type"`
	UploadedAt     *time.Time     `json:"uploaded_at"`
	LocalDeletedAt *time.Time     `json:"local_deleted_at"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
	DeletedAt      gorm.DeletedAt `json:"-" gorm:"index"`
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
}
