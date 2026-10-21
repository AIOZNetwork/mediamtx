package models

import (
	"time"

	"github.com/google/uuid"
)

// Statuses of a LiveHLSCleanup row.
const (
	// LiveHLSCleanupStatusActive: remote folder is in use by a live muxer.
	LiveHLSCleanupStatusActive = "active"
	// LiveHLSCleanupStatusPendingDelete: muxer closed, delete once DeleteAfter passes.
	LiveHLSCleanupStatusPendingDelete = "pending_delete"
	// LiveHLSCleanupStatusCleaning: claimed by a chore instance, delete in flight.
	// A row left here (instance died mid-delete) is never re-checked or requeued.
	LiveHLSCleanupStatusCleaning = "cleaning"
	// LiveHLSCleanupStatusDeleted: the provider DeleteFolder was called once. This is
	// set whatever the result; a failed call leaves its error text in LastError.
	LiveHLSCleanupStatusDeleted = "deleted"
)

// LiveHLSCleanup tracks one remote HLS folder (or DePIN contract) through its
// lifecycle so deletion survives restarts and works across instances.
// It never holds credentials. The table is created by the aioz-stream migration
// 000055_live_hls_cleanups; mediamtx only reads and writes it.
type LiveHLSCleanup struct {
	ID         uuid.UUID `json:"id" gorm:"primaryKey;type:uuid"`
	Provider   string    `json:"provider" gorm:"not null;uniqueIndex:idx_live_hls_cleanup_provider_prefix"`
	Prefix     string    `json:"prefix" gorm:"not null;uniqueIndex:idx_live_hls_cleanup_provider_prefix"`
	StreamName string    `json:"stream_name" gorm:"index"`
	// ContractID is the DePIN storage contract uuid (nil for s3/cdn).
	ContractID  *string    `json:"contract_id"`
	Status      string     `json:"status" gorm:"not null;index"`
	EndedAt     *time.Time `json:"ended_at"`
	DeleteAfter *time.Time `json:"delete_after" gorm:"index"`
	Attempts    int        `json:"attempts" gorm:"not null;default:0"`
	LastError   string     `json:"last_error"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

func (LiveHLSCleanup) TableName() string {
	return "live_hls_cleanups"
}

// LiveHLSCleanupRepository persists the cleanup lifecycle. Every method that
// transitions state is atomic, so several instances can share one table.
type LiveHLSCleanupRepository interface {
	// UpsertActive creates the row or reactivates it (reconnect): status back to
	// active, delete_after cleared. An empty contractID keeps a stored one.
	UpsertActive(provider, streamName, prefix, contractID string) error
	// MarkPendingDelete schedules deletion at deleteAfter. No-op for unknown or deleted rows
	// unless the row is created (streamName/prefix are used to create it).
	MarkPendingDelete(provider, streamName, prefix string, endedAt, deleteAfter time.Time) error
	// ClaimDue atomically moves up to limit due pending_delete rows to cleaning and returns them.
	ClaimDue(now time.Time, limit int) ([]LiveHLSCleanup, error)
	// MarkDeleted finishes a claimed row after its single DeleteFolder call.
	// lastError is the call's error text ("" on success).
	MarkDeleted(provider, prefix, lastError string) error
	// GetContract returns the live (not deleted) DePIN contract id of a stream, or "".
	GetContract(provider, streamName string) (string, error)
}
