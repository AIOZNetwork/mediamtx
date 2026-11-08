package repository

import (
	"os"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/bluenviron/mediamtx/internal/models"
)

// Needs MTX_TEST_POSTGRES_DSN (see cleanupTestRepo). The table normally comes
// from the aioz-stream migration 000056_live_hls_segments; it is created from
// the model only when missing.
func segmentTestRepo(t *testing.T) (models.LiveHLSSegmentRepository, *gorm.DB) {
	t.Helper()
	dsn := os.Getenv("MTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("MTX_TEST_POSTGRES_DSN not set")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	if !db.Migrator().HasTable(&models.LiveHLSSegment{}) {
		require.NoError(t, db.AutoMigrate(&models.LiveHLSSegment{}))
	}
	return NewLiveHLSSegmentRepository(db), db
}

func TestListUploadedObjects(t *testing.T) {
	repo, db := segmentTestRepo(t)

	stream := "lt_" + uuid.NewString() + "/240"
	other := "lt_" + uuid.NewString() + "/240"
	prefix := "live-hls/" + stream
	t.Cleanup(func() {
		db.Unscoped().Where("stream_id IN ?", []string{stream, other}).Delete(&models.LiveHLSSegment{})
	})

	insert := func(streamID, name, backend, key, etag string, deleted bool) {
		seg := models.LiveHLSSegment{
			ID: uuid.New(), StreamID: streamID, SegmentName: name, StorageBackend: backend,
			StorageKey: key, StorageETag: etag, StartedAt: time.Now(),
		}
		require.NoError(t, db.Create(&seg).Error)
		if deleted {
			require.NoError(t, db.Delete(&seg).Error)
		}
	}
	insert(stream, "a_init.mp4", "cdn", prefix+"/a_init.mp4", "id-init:0:1", false)
	insert(stream, "a_seg0.mp4", "cdn", prefix+"/a_seg0.mp4", "id-seg0:0:1", false)
	insert(stream, "a_seg1.mp4", "cdn", prefix+"/a_seg1.mp4", "id-seg1:0:1", true) // soft-deleted row, object still remote
	insert(stream, "a_seg2.mp4", "cdn", prefix+"/a_seg2.mp4", "", false)           // never uploaded
	insert(stream, "a_seg3.mp4", "s3", prefix+"/a_seg3.mp4", "etag", false)        // other backend
	insert(stream, "a_seg4.mp4", "cdn", "other/"+stream+"/a_seg4.mp4", "id-x:0:1", false)
	insert(other, "a_seg0.mp4", "cdn", "live-hls/"+other+"/a_seg0.mp4", "id-other:0:1", false)

	rows, err := repo.ListUploadedObjects("cdn", stream, prefix+"/")
	require.NoError(t, err)
	etags := make([]string, 0, len(rows))
	for _, r := range rows {
		etags = append(etags, r.StorageETag)
	}
	sort.Strings(etags)
	require.Equal(t, []string{"id-init:0:1", "id-seg0:0:1", "id-seg1:0:1"}, etags)

	// LIKE wildcards in the prefix are literal.
	rows, err = repo.ListUploadedObjects("cdn", stream, "live%")
	require.NoError(t, err)
	require.Empty(t, rows)

	_, err = repo.ListUploadedObjects("cdn", stream, "")
	require.Error(t, err)
}
