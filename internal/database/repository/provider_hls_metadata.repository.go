package repository

import (
	"errors"
	"fmt"
	"time"

	"github.com/bluenviron/mediamtx/internal/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

var (
	ErrProviderHLSIdempotencyConflict = errors.New("provider HLS metadata idempotency key conflict")
	ErrProviderHLSDuplicateObject     = errors.New("provider HLS metadata duplicate object")
	ErrProviderHLSDuplicateSequence   = errors.New("provider HLS metadata duplicate media sequence")
	ErrProviderHLSMissingInit         = errors.New("provider HLS metadata matching init segment is required before segment")
)

type ProviderHLSMetadataRepository struct {
	db *gorm.DB
}

func NewProviderHLSMetadataRepository(db *gorm.DB) models.ProviderHLSMetadataRepository {
	if db == nil {
		return nil
	}
	return &ProviderHLSMetadataRepository{db: db}
}

func (r *ProviderHLSMetadataRepository) UpsertByIdempotencyKey(metadata models.ProviderHLSMetadata) (*models.ProviderHLSMetadata, error) {
	if r == nil || r.db == nil {
		return nil, gorm.ErrRecordNotFound
	}
	metadata.NormalizeForStorage()
	if err := metadata.Validate(); err != nil {
		return nil, err
	}
	if metadata.ID == uuid.Nil {
		metadata.ID = uuid.New()
	}
	now := time.Now().UTC()

	var stored models.ProviderHLSMetadata
	err := r.db.Transaction(func(tx *gorm.DB) error {
		err := tx.Where("idempotency_key = ?", metadata.IdempotencyKey).First(&stored).Error
		if err == nil {
			if !metadata.HasSameCanonicalPayload(stored) {
				return ErrProviderHLSIdempotencyConflict
			}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		if err := ensureProviderHLSInsertAllowed(tx, metadata); err != nil {
			return err
		}

		if metadata.CreatedAt.IsZero() {
			metadata.CreatedAt = now
		}
		metadata.UpdatedAt = now
		if err := tx.Create(&metadata).Error; err != nil {
			return err
		}
		stored = metadata
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &stored, nil
}

func ensureProviderHLSInsertAllowed(tx *gorm.DB, metadata models.ProviderHLSMetadata) error {
	var count int64
	err := tx.Model(&models.ProviderHLSMetadata{}).
		Where(
			"stream_id = ? AND rendition_name = ? AND generation = ? AND kind = ? AND object_name = ?",
			metadata.StreamID, metadata.RenditionName, metadata.Generation, metadata.Kind, metadata.ObjectName,
		).
		Count(&count).Error
	if err != nil {
		return err
	}
	if count > 0 {
		return fmt.Errorf("%w: %s", ErrProviderHLSDuplicateObject, metadata.ObjectName)
	}

	if metadata.Kind == models.ProviderHLSObjectKindSegment {
		err = tx.Model(&models.ProviderHLSMetadata{}).
			Where(
				"stream_id = ? AND rendition_name = ? AND generation = ? AND kind = ? AND media_sequence = ?",
				metadata.StreamID, metadata.RenditionName, metadata.Generation, models.ProviderHLSObjectKindSegment, *metadata.MediaSequence,
			).
			Count(&count).Error
		if err != nil {
			return err
		}
		if count > 0 {
			return fmt.Errorf("%w: %d", ErrProviderHLSDuplicateSequence, *metadata.MediaSequence)
		}

		err = tx.Model(&models.ProviderHLSMetadata{}).
			Where(
				"stream_id = ? AND rendition_name = ? AND generation = ? AND kind = ? AND object_name = ? AND storage_key = ?",
				metadata.StreamID, metadata.RenditionName, metadata.Generation, models.ProviderHLSObjectKindInit,
				metadata.InitSegmentName, metadata.InitStorageKey,
			).
			Count(&count).Error
		if err != nil {
			return err
		}
		if count == 0 {
			return fmt.Errorf("%w: %s", ErrProviderHLSMissingInit, metadata.InitSegmentName)
		}
	}
	return nil
}

func (r *ProviderHLSMetadataRepository) ListWindow(query models.ProviderHLSMetadataWindowQuery) ([]models.ProviderHLSMetadata, error) {
	if r == nil || r.db == nil {
		return nil, nil
	}
	db := r.db.Where("stream_id = ? AND generation = ?", query.StreamID, query.Generation)
	if query.RenditionName != "" {
		db = db.Where("rendition_name = ?", query.RenditionName)
	}
	if query.FromMediaSequence != nil {
		db = db.Where("(kind = ? OR media_sequence >= ?)", models.ProviderHLSObjectKindInit, *query.FromMediaSequence)
	}
	if query.ToMediaSequence != nil {
		db = db.Where("(kind = ? OR media_sequence <= ?)", models.ProviderHLSObjectKindInit, *query.ToMediaSequence)
	}
	if query.SincePublishedAt != nil {
		db = db.Where("published_at >= ?", *query.SincePublishedAt)
	}
	if query.Limit > 0 {
		db = db.Limit(query.Limit)
	}

	var items []models.ProviderHLSMetadata
	err := db.Order("CASE WHEN kind = 'init' THEN 0 ELSE 1 END ASC").
		Order("CASE WHEN media_sequence IS NULL THEN 1 ELSE 0 END ASC").
		Order("media_sequence ASC").
		Order("published_at ASC").
		Find(&items).Error
	return items, err
}
