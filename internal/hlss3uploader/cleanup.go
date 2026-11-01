package hlss3uploader

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/bluenviron/mediamtx/internal/models"
)

// Remote cleanup of closed HLS folders (S3 / CDN prefixes, DePIN contracts).
//
// Design mirrors aioz-stream-core's cleanup chore, but state lives in the
// live_hls_cleanups table (models.LiveHLSCleanup, created by the aioz-stream
// migration 000055_live_hls_cleanups) instead of a JSON file:
//
//	active -> pending_delete (muxer closed, delete_after = now + gracePeriod)
//	pending_delete -> cleaning (claimed atomically by one instance)
//	cleaning -> deleted (DeleteFolder called ONCE, whatever the result)
//	pending_delete/deleted -> active (same prefix reopened)
//
// DeleteFolder is called once per row; the provider handles everything after
// the call. There is no retry. If the call returned an error the row is still
// deleted and the error text (truncated) is kept in last_error; DePIN
// not-found counts as success. A single background loop claims due
// pending_delete rows every Interval. One pass also runs at startup, so rows
// that came due while mediamtx was down are deleted right away; DePIN contract
// ids are stored in the row, so they are deleted after a restart too. A row
// left in "cleaning" (instance died mid-delete) is never re-checked.
//
// Configuration (mediamtx yml, StorageConfig.Cleanup):
//
//	storageCleanupEnabled      default yes. false: remote data is never deleted.
//	storageCleanupInterval     default 60s
//	storageCleanupGracePeriod  default 180s
//
// Without a database nothing is scheduled (a warning is logged).
const (
	DefaultCleanupInterval    = 60 * time.Second
	DefaultCleanupGracePeriod = 180 * time.Second

	cleanupBatchSize     = 10
	cleanupDeleteTimeout = 30 * time.Second
	cleanupMaxErrorLen   = 2000
)

// CleanupConfig configures the cleanup chore. Zero values mean default.
type CleanupConfig struct {
	Enabled     *bool         `json:"enabled" yaml:"enabled"`
	Interval    time.Duration `json:"interval" yaml:"interval"`
	GracePeriod time.Duration `json:"grace_period" yaml:"grace_period"`
}

func cleanupEnabled(cfg StorageConfig) bool {
	if cfg.Cleanup.Enabled != nil {
		return *cfg.Cleanup.Enabled
	}
	return true
}

func cleanupInterval(cfg StorageConfig) time.Duration {
	if cfg.Cleanup.Interval > 0 {
		return cfg.Cleanup.Interval
	}
	return DefaultCleanupInterval
}

func cleanupGracePeriod(cfg StorageConfig) time.Duration {
	if cfg.Cleanup.GracePeriod > 0 {
		return cfg.Cleanup.GracePeriod
	}
	return DefaultCleanupGracePeriod
}

var (
	cleanupRepoMu sync.RWMutex
	cleanupRepo   models.LiveHLSCleanupRepository
)

// SetCleanupRepository sets the process-wide cleanup repository (nil: no DB).
func SetCleanupRepository(repo models.LiveHLSCleanupRepository) {
	cleanupRepoMu.Lock()
	cleanupRepo = repo
	cleanupRepoMu.Unlock()
}

func defaultCleanupRepository() models.LiveHLSCleanupRepository {
	cleanupRepoMu.RLock()
	defer cleanupRepoMu.RUnlock()
	return cleanupRepo
}

// CleanupChore deletes remote folders / contracts whose grace period passed.
type CleanupChore struct {
	repo models.LiveHLSCleanupRepository
	cfg  StorageConfig

	// newProvider builds a delete-only provider for a claimed row; replaced in tests.
	newProvider func(ctx context.Context, cfg StorageConfig, row models.LiveHLSCleanup) (StorageProvider, error)
	now         func() time.Time

	cancel func()
	wg     sync.WaitGroup
}

func newCleanupChore(repo models.LiveHLSCleanupRepository, cfg StorageConfig) *CleanupChore {
	return &CleanupChore{
		repo:        repo,
		cfg:         cfg,
		newProvider: newDeleteProvider,
		now:         time.Now,
	}
}

// newDeleteProvider never creates remote resources: DePIN gets a client bound
// to the stored contract id, other providers use the normal selector.
func newDeleteProvider(ctx context.Context, cfg StorageConfig, row models.LiveHLSCleanup) (StorageProvider, error) {
	cfg.StreamName = row.StreamName
	if row.Provider == string(ProviderDePIN) {
		contractID := ""
		if row.ContractID != nil {
			contractID = *row.ContractID
		}
		return newDePINDeleteProvider(ctx, cfg, contractID)
	}
	return NewProviderSelector().Select(ctx, cfg)
}

// runPass claims due pending_delete rows and deletes each once.
func (c *CleanupChore) runPass(ctx context.Context) {
	rows, err := c.repo.ClaimDue(c.now(), cleanupBatchSize)
	if err != nil {
		slog.Warn("hls cleanup: claim failed", slog.Any("err", err))
		return
	}
	for _, row := range rows {
		if ctx.Err() != nil {
			// The rest stay in cleaning and are not re-checked.
			return
		}
		c.cleanOne(ctx, row)
	}
}

// cleanOne calls DeleteFolder once and always ends the row as deleted. A call
// error is logged and kept in last_error.
func (c *CleanupChore) cleanOne(ctx context.Context, row models.LiveHLSCleanup) {
	lastError := ""
	if err := c.deleteRow(ctx, row); err != nil {
		lastError = truncateError(err.Error())
		slog.Warn("hls cleanup: delete failed, not retrying",
			slog.String("provider", row.Provider), slog.String("prefix", row.Prefix), slog.Any("err", err))
	} else {
		slog.Info("hls cleanup: deleted remote folder",
			slog.String("provider", row.Provider), slog.String("prefix", row.Prefix))
	}
	if row.Provider == string(ProviderDePIN) {
		depinContractCache.clear(depinCacheKey(row.StreamName))
	}
	if mErr := c.repo.MarkDeleted(row.Provider, row.Prefix, lastError); mErr != nil {
		slog.Warn("hls cleanup: mark deleted error", slog.Any("err", mErr))
	}
}

func truncateError(msg string) string {
	if len(msg) <= cleanupMaxErrorLen {
		return msg
	}
	return strings.ToValidUTF8(msg[:cleanupMaxErrorLen], "")
}

func (c *CleanupChore) deleteRow(ctx context.Context, row models.LiveHLSCleanup) error {
	delCtx, cancel := context.WithTimeout(ctx, cleanupDeleteTimeout)
	defer cancel()

	provider, err := c.newProvider(delCtx, c.cfg, row)
	if err != nil {
		return err
	}
	defer provider.Close()
	if provider.Name() != row.Provider {
		// Storage config changed since the row was written. The row ends deleted with
		// this error in last_error: deleting through another provider would not remove this data.
		return &providerMismatchError{row: row.Provider, current: provider.Name()}
	}
	return provider.DeleteFolder(delCtx, row.Prefix)
}

type providerMismatchError struct{ row, current string }

func (e *providerMismatchError) Error() string {
	return "cleanup row provider " + e.row + " differs from configured provider " + e.current
}

func (c *CleanupChore) start(interval time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		// Immediately: rows that came due while mediamtx was down.
		c.runPass(ctx)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.runPass(ctx)
			}
		}
	}()
}

func (c *CleanupChore) stop() {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
}

func providerConfigured(cfg StorageConfig) bool {
	raw := getResolvedValue(cfg.Provider, "STORAGE_PROVIDER", "MTX_STORAGE_PROVIDER", "LIVE_HLS_STORAGE_PROVIDER", "")
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "local", "none":
		return false
	}
	return true
}

var (
	choreMu     sync.Mutex
	activeChore *CleanupChore
)

// StartCleanupChore starts the single cleanup loop (RunCleanupChore equivalent)
// once the database is ready. Idempotent. Needs SetCleanupRepository first.
func StartCleanupChore(cfg StorageConfig) {
	if !providerConfigured(cfg) {
		return
	}
	if !cleanupEnabled(cfg) {
		slog.Info("hls cleanup chore is disabled; remote HLS data is never deleted")
		return
	}
	repo := defaultCleanupRepository()
	if repo == nil {
		slog.Warn("hls cleanup chore not started: no database, remote HLS data is never deleted")
		return
	}
	choreMu.Lock()
	defer choreMu.Unlock()
	if activeChore != nil {
		return
	}
	interval := cleanupInterval(cfg)
	activeChore = newCleanupChore(repo, cfg)
	activeChore.start(interval)
	slog.Info("started hls cleanup chore",
		slog.Duration("interval", interval), slog.Duration("grace_period", cleanupGracePeriod(cfg)))
}

// StopCleanupChores stops the cleanup loop. Pending rows stay in the database.
func StopCleanupChores() {
	choreMu.Lock()
	c := activeChore
	activeChore = nil
	choreMu.Unlock()
	if c != nil {
		c.stop()
	}
}
