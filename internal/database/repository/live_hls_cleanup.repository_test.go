package repository

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/bluenviron/mediamtx/internal/database"
	"github.com/bluenviron/mediamtx/internal/models"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// The repository uses Postgres features (RETURNING, FOR UPDATE SKIP LOCKED), so
// tests need a real server: set MTX_TEST_POSTGRES_DSN, otherwise they skip.
func cleanupTestRepo(t *testing.T) (models.LiveHLSCleanupRepository, *gorm.DB) {
	t.Helper()
	dsn := os.Getenv("MTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("MTX_TEST_POSTGRES_DSN not set")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, database.EnsureLiveHLSCleanupTable(db))
	require.NoError(t, database.EnsureLiveHLSCleanupTable(db), "idempotent")
	require.NoError(t, db.Exec("TRUNCATE live_hls_cleanups").Error)
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

	require.NoError(t, repo.MarkFailed("depin", "live-hls/s1", "boom", now))
	r := row(t, db, "depin", "live-hls/s1")
	require.Equal(t, models.LiveHLSCleanupStatusPendingDelete, r.Status)
	require.Equal(t, 1, r.Attempts)
	require.Equal(t, "boom", r.LastError)

	rows, _ = repo.ClaimDue(now.Add(time.Minute), 10)
	require.Len(t, rows, 1)
	require.NoError(t, repo.MarkDeleted("depin", "live-hls/s1"))
	require.Equal(t, models.LiveHLSCleanupStatusDeleted, row(t, db, "depin", "live-hls/s1").Status)
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

func TestLiveHLSCleanupRequeueStale(t *testing.T) {
	repo, db := cleanupTestRepo(t)
	now := time.Now()
	require.NoError(t, repo.MarkPendingDelete("s3", "s", "p", now, now))
	_, _ = repo.ClaimDue(now, 10)
	n, err := repo.RequeueStale(now.Add(-time.Hour))
	require.NoError(t, err)
	require.Zero(t, n, "fresh claim is not stale")
	n, err = repo.RequeueStale(time.Now().Add(time.Hour))
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
	require.Equal(t, models.LiveHLSCleanupStatusPendingDelete, row(t, db, "s3", "p").Status)
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
