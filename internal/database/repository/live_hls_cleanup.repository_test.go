package repository

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/bluenviron/mediamtx/internal/models"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// The repository uses Postgres features (RETURNING, FOR UPDATE SKIP LOCKED), so
// tests need a real server: set MTX_TEST_POSTGRES_DSN, otherwise they skip.
// The table comes from the aioz-stream migration 000055_live_hls_cleanups; the
// DDL below must match it (mediamtx code never creates the table).
const cleanupTestDDL = `
DROP TABLE IF EXISTS live_hls_cleanups;
CREATE TABLE live_hls_cleanups (
	id uuid NOT NULL,
	provider text NOT NULL,
	prefix text NOT NULL,
	stream_name text,
	contract_id text,
	status text NOT NULL,
	ended_at timestamp with time zone,
	delete_after timestamp with time zone,
	attempts bigint DEFAULT 0 NOT NULL,
	last_error text,
	created_at timestamp with time zone,
	updated_at timestamp with time zone,
	CONSTRAINT live_hls_cleanups_pkey PRIMARY KEY (id)
);
CREATE UNIQUE INDEX idx_live_hls_cleanup_provider_prefix ON live_hls_cleanups (provider, prefix);
CREATE INDEX idx_live_hls_cleanups_stream_name ON live_hls_cleanups (stream_name);
CREATE INDEX idx_live_hls_cleanups_status ON live_hls_cleanups (status);
CREATE INDEX idx_live_hls_cleanups_delete_after ON live_hls_cleanups (delete_after);
`

func cleanupTestRepo(t *testing.T) (models.LiveHLSCleanupRepository, *gorm.DB) {
	t.Helper()
	dsn := os.Getenv("MTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("MTX_TEST_POSTGRES_DSN not set")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.Exec(cleanupTestDDL).Error)
	return NewLiveHLSCleanupRepository(db), db
}

func row(t *testing.T, db *gorm.DB, provider, prefix string) models.LiveHLSCleanup {
	t.Helper()
	var r models.LiveHLSCleanup
	require.NoError(t, db.Where("provider = ? AND prefix = ?", provider, prefix).First(&r).Error)
	return r
}

func TestLiveHLSCleanupLifecycle(t *testing.T) {
	repo, db := cleanupTestRepo(t)
	now := time.Now()

	require.NoError(t, repo.UpsertActive("depin", "s1", "live-hls/s1", "c-1"))
	require.NoError(t, repo.UpsertActive("depin", "s1", "live-hls/s1", ""))
	id, err := repo.GetContract("depin", "s1")
	require.NoError(t, err)
	require.Equal(t, "c-1", id, "empty contract id keeps stored one")

	require.NoError(t, repo.MarkPendingDelete("depin", "s1", "live-hls/s1", now, now.Add(time.Minute)))
	rows, err := repo.ClaimDue(now, 10)
	require.NoError(t, err)
	require.Empty(t, rows, "not due")

	rows, err = repo.ClaimDue(now.Add(2*time.Minute), 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, models.LiveHLSCleanupStatusCleaning, rows[0].Status)
	require.Equal(t, "c-1", *rows[0].ContractID)

	rows, _ = repo.ClaimDue(now.Add(2*time.Minute), 10)
	require.Empty(t, rows, "already claimed")

	require.NoError(t, repo.MarkDeleted("depin", "live-hls/s1", ""))
	r := row(t, db, "depin", "live-hls/s1")
	require.Equal(t, models.LiveHLSCleanupStatusDeleted, r.Status)
	require.Empty(t, r.LastError)
	id, _ = repo.GetContract("depin", "s1")
	require.Empty(t, id, "deleted contract not reused")

	// New stream session after delete: no stale contract carried over.
	require.NoError(t, repo.UpsertActive("depin", "s1", "live-hls/s1", ""))
	r = row(t, db, "depin", "live-hls/s1")
	require.Equal(t, models.LiveHLSCleanupStatusActive, r.Status)
	require.Nil(t, r.ContractID)
}

func TestLiveHLSCleanupReconnectCancelsPending(t *testing.T) {
	repo, db := cleanupTestRepo(t)
	now := time.Now()
	require.NoError(t, repo.MarkPendingDelete("s3", "s", "live-hls/s", now, now))
	require.NoError(t, repo.UpsertActive("s3", "s", "live-hls/s", ""))
	r := row(t, db, "s3", "live-hls/s")
	require.Equal(t, models.LiveHLSCleanupStatusActive, r.Status)
	require.Nil(t, r.DeleteAfter)
	rows, _ := repo.ClaimDue(now.Add(time.Hour), 10)
	require.Empty(t, rows)
}

func TestLiveHLSCleanupMarkPendingKeepsCleaningRow(t *testing.T) {
	repo, db := cleanupTestRepo(t)
	now := time.Now()
	require.NoError(t, repo.MarkPendingDelete("s3", "s", "p", now, now))
	rows, _ := repo.ClaimDue(now, 10)
	require.Len(t, rows, 1)
	require.NoError(t, repo.MarkPendingDelete("s3", "s", "p", now, now.Add(time.Hour)))
	require.Equal(t, models.LiveHLSCleanupStatusCleaning, row(t, db, "s3", "p").Status)
}

func TestLiveHLSCleanupCleaningRowIsNeverReclaimed(t *testing.T) {
	repo, db := cleanupTestRepo(t)
	now := time.Now()
	require.NoError(t, repo.MarkPendingDelete("s3", "s", "p", now, now))
	rows, _ := repo.ClaimDue(now, 10)
	require.Len(t, rows, 1)
	rows, err := repo.ClaimDue(now.Add(24*time.Hour), 10)
	require.NoError(t, err)
	require.Empty(t, rows)
	require.Equal(t, models.LiveHLSCleanupStatusCleaning, row(t, db, "s3", "p").Status)
}

func TestLiveHLSCleanupMarkDeletedKeepsError(t *testing.T) {
	repo, db := cleanupTestRepo(t)
	now := time.Now()
	require.NoError(t, repo.MarkPendingDelete("s3", "s", "p", now, now))
	_, _ = repo.ClaimDue(now, 10)
	require.NoError(t, repo.MarkDeleted("s3", "p", "boom"))
	r := row(t, db, "s3", "p")
	require.Equal(t, models.LiveHLSCleanupStatusDeleted, r.Status)
	require.Equal(t, "boom", r.LastError)

	// Reopen clears the error.
	require.NoError(t, repo.UpsertActive("s3", "s", "p", ""))
	require.Empty(t, row(t, db, "s3", "p").LastError)
}

func TestLiveHLSCleanupClaimIsExclusive(t *testing.T) {
	repo, _ := cleanupTestRepo(t)
	now := time.Now()
	for _, n := range []string{"a", "b", "c", "d", "e", "f"} {
		require.NoError(t, repo.MarkPendingDelete("s3", n, "live-hls/"+n, now, now))
	}
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rows, err := repo.ClaimDue(now.Add(time.Second), 2)
			require.NoError(t, err)
			mu.Lock()
			for _, r := range rows {
				seen[r.Prefix]++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	require.Len(t, seen, 6)
	for k, n := range seen {
		require.Equal(t, 1, n, k)
	}
}
