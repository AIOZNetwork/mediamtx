package database

import (
	"fmt"

	"gorm.io/gorm"
)

// liveHLSCleanupsDDL is idempotent. The authoritative schema owner is the
// aioz-stream migrations (internal/migrations); this runs only so a stream
// server started before that migration still works when its role may create
// tables. Keep it identical to models.LiveHLSCleanup.
const liveHLSCleanupsDDL = `
CREATE TABLE IF NOT EXISTS live_hls_cleanups (
	id uuid NOT NULL,
	provider text NOT NULL,
	prefix text NOT NULL,
	stream_name text,
	contract_id text,
	status text NOT NULL,
	ended_at timestamp with time zone,
	delete_after timestamp with time zone,
	attempts bigint NOT NULL DEFAULT 0,
	last_error text,
	created_at timestamp with time zone,
	updated_at timestamp with time zone,
	CONSTRAINT live_hls_cleanups_pkey PRIMARY KEY (id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_live_hls_cleanup_provider_prefix ON live_hls_cleanups (provider, prefix);
CREATE INDEX IF NOT EXISTS idx_live_hls_cleanups_stream_name ON live_hls_cleanups (stream_name);
CREATE INDEX IF NOT EXISTS idx_live_hls_cleanups_status ON live_hls_cleanups (status);
CREATE INDEX IF NOT EXISTS idx_live_hls_cleanups_delete_after ON live_hls_cleanups (delete_after);
`

// EnsureLiveHLSCleanupTable creates the cleanup table when it is missing.
func EnsureLiveHLSCleanupTable(db *gorm.DB) error {
	if db == nil {
		return nil
	}
	var exists bool
	if err := db.Raw("SELECT to_regclass('public.live_hls_cleanups') IS NOT NULL").Scan(&exists).Error; err == nil && exists {
		return nil
	}
	if err := db.Exec(liveHLSCleanupsDDL).Error; err != nil {
		return fmt.Errorf("ensure live_hls_cleanups table: %w", err)
	}
	return nil
}
