package repository

import (
	"time"

	"github.com/bluenviron/mediamtx/internal/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type LiveHLSSegmentRepository struct {
	db *gorm.DB
}

func NewLiveHLSSegmentRepository(db *gorm.DB) models.LiveHLSSegmentRepository {
	if db == nil {
		return nil
	}
	return &LiveHLSSegmentRepository{db: db}
}

func (r *LiveHLSSegmentRepository) upsert(segment models.LiveHLSSegment, status string, cols []string) error {
	if r == nil || r.db == nil {
		return nil
	}
	if segment.ID == uuid.Nil {
		segment.ID = uuid.New()
	}
	segment.Status = status
	return r.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "stream_id"}, {Name: "segment_name"}},
		DoUpdates: clause.AssignmentColumns(cols),
	}).Create(&segment).Error
}

func (r *LiveHLSSegmentRepository) UpsertLocal(segment models.LiveHLSSegment) error {
	return r.upsert(segment, models.LiveHLSSegmentStatusLocal, []string{
		"local_path", "storage_backend", "storage_key", "duration_ms", "size_bytes", "sequence", "content_type", "status", "updated_at",
	})
}

func (r *LiveHLSSegmentRepository) UpsertUploading(segment models.LiveHLSSegment) error {
	return r.upsert(segment, models.LiveHLSSegmentStatusUploading, []string{
		"local_path", "storage_backend", "storage_key", "duration_ms", "size_bytes", "sequence", "content_type", "status", "updated_at",
	})
}

func (r *LiveHLSSegmentRepository) UpsertUploaded(segment models.LiveHLSSegment) error {
	return r.upsert(segment, models.LiveHLSSegmentStatusUploadedS3, []string{
		"local_path", "storage_backend", "storage_key", "storage_e_tag", "duration_ms", "size_bytes", "sequence", "content_type", "uploaded_at", "status", "updated_at",
	})
}

func (r *LiveHLSSegmentRepository) UpsertUploadFailed(segment models.LiveHLSSegment) error {
	return r.upsert(segment, models.LiveHLSSegmentStatusUploadFailed, []string{
		"local_path", "storage_backend", "storage_key", "duration_ms", "size_bytes", "sequence", "content_type", "status", "updated_at",
	})
}

func (r *LiveHLSSegmentRepository) MarkLocalDeleted(streamID, segmentName string, deletedAt time.Time) error {
	if r == nil || r.db == nil {
		return nil
	}
	return r.db.Model(&models.LiveHLSSegment{}).
		Where("stream_id = ? AND segment_name = ?", streamID, segmentName).
		Updates(map[string]interface{}{"local_deleted_at": deletedAt}).Error
}

func (r *LiveHLSSegmentRepository) GetByStorageKey(storageKey string) (*models.LiveHLSSegment, error) {
	if r == nil || r.db == nil {
		return nil, gorm.ErrRecordNotFound
	}
	var segment models.LiveHLSSegment
	err := r.db.Where("storage_key = ?", storageKey).First(&segment).Error
	if err != nil {
		return nil, err
	}
	return &segment, nil
}
