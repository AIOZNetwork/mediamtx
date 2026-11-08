package hlss3uploader

import (
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bluenviron/mediamtx/internal/models"
)

// fakeStoredObjects returns the uploaded live_hls_segments rows of a prefix.
type fakeStoredObjects struct {
	rows     []models.LiveHLSSegment
	backend  string
	streamID string
	prefix   string
}

func (f *fakeStoredObjects) ListUploadedObjects(backend, streamID, prefix string) ([]models.LiveHLSSegment, error) {
	f.backend, f.streamID, f.prefix = backend, streamID, prefix
	return f.rows, nil
}

// fakeCDN answers DELETE /endFileRecord and records the deleted file ids.
func fakeCDN(t *testing.T) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var deleted []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && r.URL.Path == "/endFileRecord" {
			mu.Lock()
			deleted = append(deleted, r.URL.Query().Get("file_record_id"))
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []string {
		mu.Lock()
		defer mu.Unlock()
		out := append([]string(nil), deleted...)
		sort.Strings(out)
		return out
	}
}

// The chore builds a fresh CDN provider for each row, so the provider has
// never seen the stream's uploads. Before the fix its DeleteFolder found no
// file records, deleted nothing, and the row still ended as deleted. The
// file ids must come from the live_hls_segments rows of the prefix.
func TestCleanupChoreCDNDeletesUploadedFileRecords(t *testing.T) {
	srv, deletedIDs := fakeCDN(t)
	t.Setenv("STORAGE_PROVIDER", "")

	prefix := "live-hls/64828ec8/240"
	repo := newFakeCleanupRepo()
	now := time.Now()
	require.NoError(t, repo.MarkPendingDelete("cdn", "64828ec8", prefix, now, now.Add(-time.Second)))

	stored := &fakeStoredObjects{rows: []models.LiveHLSSegment{
		{StorageKey: prefix + "/a_video1_init.mp4", StorageETag: "id-init:0:946"},
		{StorageKey: prefix + "/a_video1_seg0.mp4", StorageETag: "id-seg0:0:37178"},
		{StorageKey: prefix + "/a_audio2_seg0.mp4", StorageETag: "id-seg1:0:292"},
	}}

	chore := newCleanupChore(repo, StorageConfig{Provider: "cdn", CDNEndpoint: srv.URL, CDNHubURL: srv.URL})
	chore.stored = stored
	chore.runPass(t.Context())

	require.Equal(t, []string{"id-init", "id-seg0", "id-seg1"}, deletedIDs())
	require.Equal(t, "cdn", stored.backend)
	require.Equal(t, "64828ec8", stored.streamID)
	require.Equal(t, prefix, stored.prefix)
	row := repo.get("cdn", prefix)
	require.Equal(t, models.LiveHLSCleanupStatusDeleted, row.Status)
	require.Empty(t, row.LastError)
}

// Without the segment repository the CDN ids cannot be known: the row must
// carry an error instead of looking like a successful delete.
func TestCleanupChoreCDNWithoutSegmentRepositoryRecordsError(t *testing.T) {
	srv, deletedIDs := fakeCDN(t)
	t.Setenv("STORAGE_PROVIDER", "")

	prefix := "live-hls/64828ec8/240"
	repo := newFakeCleanupRepo()
	now := time.Now()
	require.NoError(t, repo.MarkPendingDelete("cdn", "64828ec8", prefix, now, now.Add(-time.Second)))

	chore := newCleanupChore(repo, StorageConfig{Provider: "cdn", CDNEndpoint: srv.URL, CDNHubURL: srv.URL})
	chore.runPass(t.Context())

	require.Empty(t, deletedIDs())
	require.NotEmpty(t, repo.get("cdn", prefix).LastError)
}
