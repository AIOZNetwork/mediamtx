package hlss3uploader

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bluenviron/mediamtx/internal/models"
	"github.com/stretchr/testify/require"
)

// fakeCleanupRepo is an in-memory models.LiveHLSCleanupRepository with the
// same transition rules as the gorm one. Claims are exclusive (mutex).
type fakeCleanupRepo struct {
	mu   sync.Mutex
	rows map[string]*models.LiveHLSCleanup
	now  func() time.Time
}

func newFakeCleanupRepo() *fakeCleanupRepo {
	return &fakeCleanupRepo{rows: map[string]*models.LiveHLSCleanup{}, now: time.Now}
}

func fkey(provider, prefix string) string { return provider + "|" + prefix }

func (f *fakeCleanupRepo) get(provider, prefix string) *models.LiveHLSCleanup {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r, ok := f.rows[fkey(provider, prefix)]; ok {
		c := *r
		return &c
	}
	return nil
}

func (f *fakeCleanupRepo) UpsertActive(provider, streamName, prefix, contractID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.rows[fkey(provider, prefix)]
	if !ok {
		r = &models.LiveHLSCleanup{Provider: provider, Prefix: prefix}
		f.rows[fkey(provider, prefix)] = r
	}
	if contractID != "" {
		r.ContractID = &contractID
	} else if r.Status == models.LiveHLSCleanupStatusDeleted {
		r.ContractID = nil
	}
	r.StreamName = streamName
	r.Status = models.LiveHLSCleanupStatusActive
	r.DeleteAfter, r.EndedAt, r.Attempts, r.LastError = nil, nil, 0, ""
	return nil
}

func (f *fakeCleanupRepo) MarkPendingDelete(provider, streamName, prefix string, endedAt, deleteAfter time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.rows[fkey(provider, prefix)]
	if !ok {
		r = &models.LiveHLSCleanup{Provider: provider, Prefix: prefix, StreamName: streamName}
		f.rows[fkey(provider, prefix)] = r
	}
	if r.Status == models.LiveHLSCleanupStatusCleaning {
		return nil
	}
	r.Status = models.LiveHLSCleanupStatusPendingDelete
	r.EndedAt, r.DeleteAfter, r.Attempts = &endedAt, &deleteAfter, 0
	return nil
}

func (f *fakeCleanupRepo) ClaimDue(now time.Time, limit int) ([]models.LiveHLSCleanup, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []models.LiveHLSCleanup
	for _, r := range f.rows {
		if len(out) >= limit {
			break
		}
		if r.Status == models.LiveHLSCleanupStatusPendingDelete && r.DeleteAfter != nil && !r.DeleteAfter.After(now) {
			r.Status = models.LiveHLSCleanupStatusCleaning
			r.UpdatedAt = now
			out = append(out, *r)
		}
	}
	return out, nil
}

func (f *fakeCleanupRepo) MarkDeleted(provider, prefix, lastError string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r := f.rows[fkey(provider, prefix)]; r != nil && r.Status == models.LiveHLSCleanupStatusCleaning {
		r.Status = models.LiveHLSCleanupStatusDeleted
		r.LastError = lastError
	}
	return nil
}

func (f *fakeCleanupRepo) GetContract(provider, streamName string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.rows {
		if r.Provider == provider && r.StreamName == streamName && r.ContractID != nil && r.Status != models.LiveHLSCleanupStatusDeleted {
			return *r.ContractID, nil
		}
	}
	return "", nil
}

// cleanupMock records deletes. name overrides Name().
type cleanupMock struct {
	mockProvider
	mu      sync.Mutex
	name    string
	deleted []string
	calls   int
	failN   int
}

func (m *cleanupMock) Name() string {
	if m.name != "" {
		return m.name
	}
	return "mock"
}

func (m *cleanupMock) DeleteFolder(_ context.Context, prefix string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.failN > 0 {
		m.failN--
		return errors.New("boom")
	}
	m.deleted = append(m.deleted, prefix)
	return nil
}

func (m *cleanupMock) deletedCopy() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.deleted...)
}

type chorePair struct {
	chore *CleanupChore
	repo  *fakeCleanupRepo
	prov  *cleanupMock
	now   *time.Time
	rows  []models.LiveHLSCleanup // rows handed to newProvider
}

func newChorePair(t *testing.T, repo *fakeCleanupRepo, prov *cleanupMock) *chorePair {
	t.Helper()
	p := &chorePair{repo: repo, prov: prov}
	now := time.Now()
	p.now = &now
	p.chore = newCleanupChore(repo, StorageConfig{})
	p.chore.now = func() time.Time { return *p.now }
	p.chore.newProvider = func(_ context.Context, _ StorageConfig, row models.LiveHLSCleanup) (StorageProvider, error) {
		p.rows = append(p.rows, row)
		return prov, nil
	}
	return p
}

func TestCleanupChoreDeletesOnlyWhenDue(t *testing.T) {
	repo := newFakeCleanupRepo()
	p := newChorePair(t, repo, &cleanupMock{})
	require.NoError(t, repo.MarkPendingDelete("mock", "s", "live-hls/s", *p.now, p.now.Add(time.Minute)))

	p.chore.runPass(context.Background())
	require.Empty(t, p.prov.deletedCopy())

	*p.now = p.now.Add(2 * time.Minute)
	p.chore.runPass(context.Background())
	require.Equal(t, []string{"live-hls/s"}, p.prov.deletedCopy())
	require.Equal(t, models.LiveHLSCleanupStatusDeleted, repo.get("mock", "live-hls/s").Status)
}

func TestCleanupChoreReconnectCancels(t *testing.T) {
	repo := newFakeCleanupRepo()
	p := newChorePair(t, repo, &cleanupMock{})
	require.NoError(t, repo.MarkPendingDelete("mock", "s", "live-hls/s", *p.now, p.now.Add(time.Minute)))
	require.NoError(t, repo.UpsertActive("mock", "s", "live-hls/s", ""))

	*p.now = p.now.Add(time.Hour)
	p.chore.runPass(context.Background())
	require.Empty(t, p.prov.deletedCopy())
	row := repo.get("mock", "live-hls/s")
	require.Equal(t, models.LiveHLSCleanupStatusActive, row.Status)
	require.Nil(t, row.DeleteAfter)
}

func TestCleanupChoreSuccessHasEmptyLastError(t *testing.T) {
	repo := newFakeCleanupRepo()
	prov := &cleanupMock{}
	p := newChorePair(t, repo, prov)
	require.NoError(t, repo.MarkPendingDelete("mock", "s", "live-hls/s", *p.now, *p.now))

	p.chore.runPass(context.Background())
	row := repo.get("mock", "live-hls/s")
	require.Equal(t, models.LiveHLSCleanupStatusDeleted, row.Status)
	require.Empty(t, row.LastError)
	require.Equal(t, 1, prov.calls)
}

func TestCleanupChoreErrorStillEndsDeletedWithLastError(t *testing.T) {
	repo := newFakeCleanupRepo()
	prov := &cleanupMock{failN: 20}
	p := newChorePair(t, repo, prov)
	require.NoError(t, repo.MarkPendingDelete("mock", "s", "live-hls/s", *p.now, *p.now))

	for i := 0; i < 5; i++ {
		p.chore.runPass(context.Background())
		*p.now = p.now.Add(time.Hour)
	}
	row := repo.get("mock", "live-hls/s")
	require.Equal(t, models.LiveHLSCleanupStatusDeleted, row.Status)
	require.Equal(t, "boom", row.LastError)
	require.Equal(t, 1, prov.calls, "exactly one call, no retry")
}

func TestCleanupChoreProviderMismatchEndsDeletedWithError(t *testing.T) {
	repo := newFakeCleanupRepo()
	prov := &cleanupMock{name: "s3"}
	p := newChorePair(t, repo, prov)
	require.NoError(t, repo.MarkPendingDelete("depin", "s", "live-hls/s", *p.now, *p.now))

	p.chore.runPass(context.Background())
	row := repo.get("depin", "live-hls/s")
	require.Equal(t, models.LiveHLSCleanupStatusDeleted, row.Status)
	require.NotEmpty(t, row.LastError)
	require.Zero(t, prov.calls)
}

func TestCleanupErrorTruncated(t *testing.T) {
	long := strings.Repeat("x", cleanupMaxErrorLen+500)
	require.Len(t, truncateError(long), cleanupMaxErrorLen)
	require.Equal(t, "short", truncateError("short"))
}

func TestCleanupChoreDePINUsesStoredContractID(t *testing.T) {
	repo := newFakeCleanupRepo()
	prov := &cleanupMock{name: "depin"}
	p := newChorePair(t, repo, prov)
	require.NoError(t, repo.UpsertActive("depin", "stream1", "live-hls/stream1", "contract-123"))
	require.NoError(t, repo.MarkPendingDelete("depin", "stream1", "live-hls/stream1", *p.now, *p.now))

	p.chore.runPass(context.Background())
	require.Len(t, p.rows, 1)
	require.NotNil(t, p.rows[0].ContractID)
	require.Equal(t, "contract-123", *p.rows[0].ContractID)
	require.Equal(t, 1, len(prov.deletedCopy()))
	require.Equal(t, models.LiveHLSCleanupStatusDeleted, repo.get("depin", "live-hls/stream1").Status)
	id, _ := repo.GetContract("depin", "stream1")
	require.Empty(t, id, "deleted contract is not reused")
}

func TestCleanupChoreRestartDeletesOverdue(t *testing.T) {
	repo := newFakeCleanupRepo()
	t0 := time.Now()
	require.NoError(t, repo.MarkPendingDelete("mock", "s", "live-hls/s", t0.Add(-time.Hour), t0.Add(-50*time.Minute)))

	// New chore = restarted process; state only in the repo. start() runs a pass at once.
	prov := &cleanupMock{}
	p := newChorePair(t, repo, prov)
	p.chore.now = time.Now
	p.chore.start(time.Hour)
	defer p.chore.stop()
	require.Eventually(t, func() bool { return len(prov.deletedCopy()) == 1 }, 2*time.Second, 10*time.Millisecond)
}

func TestCleanupChoreNeverReclaimsCleaningRows(t *testing.T) {
	repo := newFakeCleanupRepo()
	p := newChorePair(t, repo, &cleanupMock{})
	require.NoError(t, repo.MarkPendingDelete("mock", "s", "live-hls/s", *p.now, *p.now))
	_, err := repo.ClaimDue(*p.now, 10) // claimed, then the instance "died"
	require.NoError(t, err)

	*p.now = p.now.Add(24 * time.Hour)
	p.chore.runPass(context.Background())
	require.Zero(t, p.prov.calls)
	require.Equal(t, models.LiveHLSCleanupStatusCleaning, repo.get("mock", "live-hls/s").Status)
}

func TestCleanupChoreClaimIsExclusive(t *testing.T) {
	repo := newFakeCleanupRepo()
	now := time.Now()
	for _, n := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, repo.MarkPendingDelete("mock", n, "live-hls/"+n, now, now))
	}
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rows, _ := repo.ClaimDue(now, 2)
			mu.Lock()
			for _, r := range rows {
				seen[r.Prefix]++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	require.Len(t, seen, 5)
	for k, n := range seen {
		require.Equal(t, 1, n, k)
	}
}

func TestCleanupConfigResolution(t *testing.T) {
	cfg := StorageConfig{}
	require.True(t, cleanupEnabled(cfg))
	require.Equal(t, DefaultCleanupInterval, cleanupInterval(cfg))
	require.Equal(t, DefaultCleanupGracePeriod, cleanupGracePeriod(cfg))

	off := false
	cfg.Cleanup = CleanupConfig{Enabled: &off, Interval: 5 * time.Second, GracePeriod: 4 * time.Minute}
	require.False(t, cleanupEnabled(cfg))
	require.Equal(t, 5*time.Second, cleanupInterval(cfg))
	require.Equal(t, 4*time.Minute, cleanupGracePeriod(cfg))
}
