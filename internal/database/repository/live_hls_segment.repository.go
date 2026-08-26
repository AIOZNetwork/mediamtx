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
	if segment.StartedAt.IsZero() {
		cols = withoutColumn(cols, "started_at")
	}
	return r.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "stream_id"}, {Name: "segment_name"}},
		DoUpdates: clause.AssignmentColumns(cols),
	}).Create(&segment).Error
}

func withoutColumn(cols []string, column string) []string {
	out := cols[:0]
	for _, col := range cols {
		if col != column {
			out = append(out, col)
		}
	}
	return out
}

func (r *LiveHLSSegmentRepository) UpsertLocal(segment models.LiveHLSSegment) error {
	return r.upsert(segment, models.LiveHLSSegmentStatusLocal, []string{
		"base_stream_id", "session_id", "mux_session_id", "track_type", "rendition", "init_segment_name", "init_storage_key", "playlist_name", "local_path", "storage_backend", "storage_key", "started_at", "duration_ms", "size_bytes", "sequence", "media_sequence", "content_type", "status", "updated_at",
	})
}

func (r *LiveHLSSegmentRepository) UpsertUploading(segment models.LiveHLSSegment) error {
	return r.upsert(segment, models.LiveHLSSegmentStatusUploading, []string{
		"base_stream_id", "session_id", "mux_session_id", "track_type", "rendition", "init_segment_name", "init_storage_key", "playlist_name", "local_path", "storage_backend", "storage_key", "started_at", "duration_ms", "size_bytes", "sequence", "media_sequence", "content_type", "status", "updated_at",
	})
}

func (r *LiveHLSSegmentRepository) UpsertUploaded(segment models.LiveHLSSegment) error {
	return r.upsert(segment, models.LiveHLSSegmentStatusUploadedS3, []string{
		"base_stream_id", "session_id", "mux_session_id", "track_type", "rendition", "init_segment_name", "init_storage_key", "playlist_name", "local_path", "storage_backend", "storage_key", "storage_e_tag", "started_at", "duration_ms", "size_bytes", "sequence", "media_sequence", "content_type", "uploaded_at", "status", "updated_at",
	})
}

func (r *LiveHLSSegmentRepository) UpsertUploadFailed(segment models.LiveHLSSegment) error {
	return r.upsert(segment, models.LiveHLSSegmentStatusUploadFailed, []string{
		"base_stream_id", "session_id", "mux_session_id", "track_type", "rendition", "init_segment_name", "init_storage_key", "playlist_name", "local_path", "storage_backend", "storage_key", "started_at", "duration_ms", "size_bytes", "sequence", "media_sequence", "content_type", "status", "updated_at",
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

func (r *LiveHLSSegmentRepository) GetByStreamAndSegment(streamID, segmentName string) (*models.LiveHLSSegment, error) {
	if r == nil || r.db == nil {
		return nil, gorm.ErrRecordNotFound
	}
	var segment models.LiveHLSSegment
	err := r.db.Where("stream_id = ? AND segment_name = ?", streamID, segmentName).First(&segment).Error
	if err != nil {
		return nil, err
	}
	return &segment, nil
}

func (r *LiveHLSSegmentRepository) ListWindow(streamID, sessionID string, since time.Time) ([]models.LiveHLSSegment, error) {
	if r == nil || r.db == nil {
		return nil, nil
	}
	var segments []models.LiveHLSSegment
	query := r.db.Where(
		"stream_id = ? AND started_at >= ? AND status IN ?",
		streamID,
		since,
		[]string{models.LiveHLSSegmentStatusLocal, models.LiveHLSSegmentStatusUploadedS3},
	)
	if sessionID != "" {
		query = query.Where("session_id = ?", sessionID)
	}
	err := query.Order("sequence ASC, started_at ASC").Find(&segments).Error
	return segments, err
}

func (r *LiveHLSSegmentRepository) ListFMP4Window(streamID string, since time.Time) ([]models.LiveHLSSegment, error) {
	if r == nil || r.db == nil {
		return nil, nil
	}
	var segments []models.LiveHLSSegment
	err := r.db.Where(
		"stream_id = ? AND started_at >= ? AND status IN ?",
		streamID,
		since,
		[]string{models.LiveHLSSegmentStatusLocal, models.LiveHLSSegmentStatusUploadedS3},
	).
		Order("CASE WHEN media_sequence IS NULL THEN 1 ELSE 0 END ASC").
		Order("media_sequence ASC").
		Order("started_at ASC").
		Order("sequence ASC").
		Find(&segments).Error
	return segments, err
}

func (r *LiveHLSSegmentRepository) MaxSequence(streamID string) (int64, bool, error) {
	if r == nil || r.db == nil {
		return 0, false, nil
	}
	var segment models.LiveHLSSegment
	err := r.db.Where("stream_id = ? AND sequence IS NOT NULL", streamID).
		Order("sequence DESC").First(&segment).Error
	if err == gorm.ErrRecordNotFound {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if segment.Sequence == nil {
		return 0, false, nil
	}
	return *segment.Sequence, true, nil
}
