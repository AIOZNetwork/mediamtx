package repository

import (
	"time"

	"github.com/bluenviron/mediamtx/internal/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type LiveHLSCleanupRepository struct {
	db *gorm.DB
}

func NewLiveHLSCleanupRepository(db *gorm.DB) models.LiveHLSCleanupRepository {
	if db == nil {
		return nil
	}
	return &LiveHLSCleanupRepository{db: db}
}

func (r *LiveHLSCleanupRepository) UpsertActive(provider, streamName, prefix, contractID string) error {
	row := models.LiveHLSCleanup{
		ID:         uuid.New(),
		Provider:   provider,
		Prefix:     prefix,
		StreamName: streamName,
		Status:     models.LiveHLSCleanupStatusActive,
	}
	if contractID != "" {
		row.ContractID = &contractID
	}
	return r.db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "provider"}, {Name: "prefix"}},
		DoUpdates: clause.Set{
			{Column: clause.Column{Name: "stream_name"}, Value: gorm.Expr("EXCLUDED.stream_name")},
			{Column: clause.Column{Name: "status"}, Value: models.LiveHLSCleanupStatusActive},
			{Column: clause.Column{Name: "delete_after"}, Value: nil},
			{Column: clause.Column{Name: "ended_at"}, Value: nil},
			{Column: clause.Column{Name: "attempts"}, Value: 0},
			{Column: clause.Column{Name: "last_error"}, Value: ""},
			{Column: clause.Column{Name: "updated_at"}, Value: gorm.Expr("EXCLUDED.updated_at")},
			// New id wins; otherwise keep the stored one unless it belongs to a deleted contract.
			{Column: clause.Column{Name: "contract_id"}, Value: gorm.Expr(
				"CASE WHEN EXCLUDED.contract_id IS NOT NULL THEN EXCLUDED.contract_id " +
					"WHEN live_hls_cleanups.status = 'deleted' THEN NULL " +
					"ELSE live_hls_cleanups.contract_id END")},
		},
	}).Create(&row).Error
}

func (r *LiveHLSCleanupRepository) MarkPendingDelete(provider, streamName, prefix string, endedAt, deleteAfter time.Time) error {
	row := models.LiveHLSCleanup{
		ID:          uuid.New(),
		Provider:    provider,
		Prefix:      prefix,
		StreamName:  streamName,
		Status:      models.LiveHLSCleanupStatusPendingDelete,
		EndedAt:     &endedAt,
		DeleteAfter: &deleteAfter,
	}
	// A claimed (cleaning) row is left alone; everything else becomes pending_delete.
	return r.db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "provider"}, {Name: "prefix"}},
		DoUpdates: clause.Set{
			{Column: clause.Column{Name: "status"}, Value: models.LiveHLSCleanupStatusPendingDelete},
			{Column: clause.Column{Name: "ended_at"}, Value: endedAt},
			{Column: clause.Column{Name: "delete_after"}, Value: deleteAfter},
			{Column: clause.Column{Name: "attempts"}, Value: 0},
			{Column: clause.Column{Name: "updated_at"}, Value: gorm.Expr("EXCLUDED.updated_at")},
		},
		Where: clause.Where{Exprs: []clause.Expression{
			gorm.Expr("live_hls_cleanups.status <> ?", models.LiveHLSCleanupStatusCleaning),
		}},
	}).Create(&row).Error
}

func (r *LiveHLSCleanupRepository) ClaimDue(now time.Time, limit int) ([]models.LiveHLSCleanup, error) {
	if limit <= 0 {
		limit = 10
	}
	var rows []models.LiveHLSCleanup
	err := r.db.Raw(`
UPDATE live_hls_cleanups SET status = ?, updated_at = ?
WHERE id IN (
	SELECT id FROM live_hls_cleanups
	WHERE status = ? AND delete_after <= ?
	ORDER BY delete_after
	LIMIT ?
	FOR UPDATE SKIP LOCKED
)
RETURNING *`,
		models.LiveHLSCleanupStatusCleaning, now,
		models.LiveHLSCleanupStatusPendingDelete, now, limit,
	).Scan(&rows).Error
	return rows, err
}

func (r *LiveHLSCleanupRepository) MarkDeleted(provider, prefix, lastError string) error {
	return r.db.Model(&models.LiveHLSCleanup{}).
		Where("provider = ? AND prefix = ? AND status = ?", provider, prefix, models.LiveHLSCleanupStatusCleaning).
		Updates(map[string]interface{}{
			"status":     models.LiveHLSCleanupStatusDeleted,
			"last_error": lastError,
			"updated_at": time.Now(),
		}).Error
}

func (r *LiveHLSCleanupRepository) GetContract(provider, streamName string) (string, error) {
	var row models.LiveHLSCleanup
	err := r.db.
		Where("provider = ? AND stream_name = ? AND contract_id IS NOT NULL AND status <> ?",
			provider, streamName, models.LiveHLSCleanupStatusDeleted).
		Order("updated_at DESC").
		Limit(1).
		Find(&row).Error
	if err != nil || row.ContractID == nil {
		return "", err
	}
	return *row.ContractID, nil
}
