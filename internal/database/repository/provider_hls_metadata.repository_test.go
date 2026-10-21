package repository

import (
	"errors"
	"testing"
	"time"

	"github.com/bluenviron/mediamtx/internal/models"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func newProviderHLSTestRepo(t *testing.T) models.ProviderHLSMetadataRepository {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.ProviderHLSMetadata{}))
	return NewProviderHLSMetadataRepository(db)
}

func validProviderHLSInit(key string) models.ProviderHLSMetadata {
	return models.ProviderHLSMetadata{
		EventID:        "event-1",
		IdempotencyKey: key,
		StreamID:       "stream-1",
		MediaMTXPath:   "live/stream-1",
		JobID:          "job-1",
		Generation:     1,
		RenditionName:  "main",
		Width:          1280,
		Height:         720,
		Bandwidth:      2500000,
		Codecs:         "avc1.64001f,mp4a.40.2",
		Kind:           models.ProviderHLSObjectKindInit,
		ObjectName:     "init.mp4",
		Provider:       "depin",
		ContractID:     "contract-1",
		StorageKey:     "streams/stream-1/init.mp4",
		URL:            "https://example.test/init.mp4",
		ObjectID:       "object-init",
		ContentType:    "video/mp4",
		SizeBytes:      100,
		PublishedAt:    time.Unix(100, 0).UTC(),
	}
}

func validProviderHLSSegment(key string, seq int64, name string) models.ProviderHLSMetadata {
	return models.ProviderHLSMetadata{
		EventID:         "event-1",
		IdempotencyKey:  key,
		StreamID:        "stream-1",
		MediaMTXPath:    "live/stream-1",
		JobID:           "job-1",
		Generation:      1,
		RenditionName:   "main",
		Width:           1280,
		Height:          720,
		Bandwidth:       2500000,
		Codecs:          "avc1.64001f,mp4a.40.2",
		Kind:            models.ProviderHLSObjectKindSegment,
		ObjectName:      name,
		Provider:        "depin",
		ContractID:      "contract-1",
		StorageKey:      "streams/stream-1/" + name,
		URL:             "https://example.test/" + name,
		ObjectID:        "object-" + name,
		ContentType:     "video/iso.segment",
		SizeBytes:       1000,
		MediaSequence:   &seq,
		DurationMS:      2000,
		InitSegmentName: "init.mp4",
		InitStorageKey:  "streams/stream-1/init.mp4",
		PublishedAt:     time.Unix(100+seq, 0).UTC(),
	}
}

func TestProviderHLSMetadataIdempotencyImmutable(t *testing.T) {
	repo := newProviderHLSTestRepo(t)
	init := validProviderHLSInit("idem-1")
	stored, err := repo.UpsertByIdempotencyKey(init)
	require.NoError(t, err)
	require.Equal(t, "streams/stream-1/init.mp4", stored.StorageKey)

	stored, err = repo.UpsertByIdempotencyKey(init)
	require.NoError(t, err)
	require.Equal(t, "streams/stream-1/init.mp4", stored.StorageKey)

	divergent := init
	divergent.StorageKey = "streams/stream-1/other-init.mp4"
	_, err = repo.UpsertByIdempotencyKey(divergent)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrProviderHLSIdempotencyConflict))

	items, err := repo.ListWindow(models.ProviderHLSMetadataWindowQuery{StreamID: "stream-1", RenditionName: "main", Generation: 1})
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, "streams/stream-1/init.mp4", items[0].StorageKey)
}

func TestProviderHLSMetadataIdempotencyNormalizesTimestampPrecision(t *testing.T) {
	repo := newProviderHLSTestRepo(t)
	init := validProviderHLSInit("idem-time")
	init.PublishedAt = time.Date(2026, 10, 2, 13, 0, 0, 123456789, time.FixedZone("ICT", 7*60*60))
	pdt := time.Date(2026, 10, 2, 13, 0, 1, 987654321, time.FixedZone("ICT", 7*60*60))
	init.ProgramDateTime = &pdt

	stored, err := repo.UpsertByIdempotencyKey(init)
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 10, 2, 6, 0, 0, 123456000, time.UTC), stored.PublishedAt)
	require.NotNil(t, stored.ProgramDateTime)
	require.Equal(t, time.Date(2026, 10, 2, 6, 0, 1, 987654000, time.UTC), *stored.ProgramDateTime)

	replayed, err := repo.UpsertByIdempotencyKey(init)
	require.NoError(t, err)
	require.Equal(t, stored.ID, replayed.ID)
}

func TestProviderHLSMetadataInitBeforeSegmentAndOrdering(t *testing.T) {
	repo := newProviderHLSTestRepo(t)
	seq1 := int64(1)
	_, err := repo.UpsertByIdempotencyKey(validProviderHLSSegment("seg-before-init", seq1, "seg-1.m4s"))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrProviderHLSMissingInit))

	_, err = repo.UpsertByIdempotencyKey(validProviderHLSInit("init"))
	require.NoError(t, err)
	_, err = repo.UpsertByIdempotencyKey(validProviderHLSSegment("seg-2", 2, "seg-2.m4s"))
	require.NoError(t, err)
	_, err = repo.UpsertByIdempotencyKey(validProviderHLSSegment("seg-1", 1, "seg-1.m4s"))
	require.NoError(t, err)

	from := int64(1)
	to := int64(2)
	items, err := repo.ListWindow(models.ProviderHLSMetadataWindowQuery{
		StreamID:          "stream-1",
		RenditionName:     "main",
		Generation:        1,
		FromMediaSequence: &from,
		ToMediaSequence:   &to,
	})
	require.NoError(t, err)
	require.Len(t, items, 3)
	require.Equal(t, models.ProviderHLSObjectKindInit, items[0].Kind)
	require.Equal(t, int64(1), *items[1].MediaSequence)
	require.Equal(t, int64(2), *items[2].MediaSequence)
}

func TestProviderHLSMetadataLogicalDuplicates(t *testing.T) {
	repo := newProviderHLSTestRepo(t)
	_, err := repo.UpsertByIdempotencyKey(validProviderHLSInit("init"))
	require.NoError(t, err)

	dupObject := validProviderHLSInit("init-duplicate-object")
	_, err = repo.UpsertByIdempotencyKey(dupObject)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrProviderHLSDuplicateObject))

	_, err = repo.UpsertByIdempotencyKey(validProviderHLSSegment("seg-1", 1, "seg-1.m4s"))
	require.NoError(t, err)
	_, err = repo.UpsertByIdempotencyKey(validProviderHLSSegment("seg-1-duplicate-seq", 1, "seg-1b.m4s"))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrProviderHLSDuplicateSequence))
}

func TestProviderHLSMetadataValidation(t *testing.T) {
	repo := newProviderHLSTestRepo(t)

	bad := validProviderHLSInit("bad-generation")
	bad.Generation = 0
	_, err := repo.UpsertByIdempotencyKey(bad)
	require.Error(t, err)

	bad = validProviderHLSInit("bad-kind")
	bad.Kind = "playlist"
	_, err = repo.UpsertByIdempotencyKey(bad)
	require.Error(t, err)

	bad = validProviderHLSInit("bad-codecs")
	bad.Codecs = ""
	_, err = repo.UpsertByIdempotencyKey(bad)
	require.Error(t, err)

	seg := validProviderHLSSegment("bad-segment", 1, "seg-1.m4s")
	seg.DurationMS = 0
	_, err = repo.UpsertByIdempotencyKey(seg)
	require.Error(t, err)

	seg = validProviderHLSSegment("bad-segment-init", 1, "seg-1.m4s")
	seg.InitStorageKey = ""
	_, err = repo.UpsertByIdempotencyKey(seg)
	require.Error(t, err)
}

func TestProviderHLSMetadataDeleteByStreamID(t *testing.T) {
	repo := newProviderHLSTestRepo(t)
	_, err := repo.UpsertByIdempotencyKey(validProviderHLSInit("init"))
	require.NoError(t, err)
	_, err = repo.UpsertByIdempotencyKey(validProviderHLSSegment("seg-1", 1, "seg-1.m4s"))
	require.NoError(t, err)

	n, err := repo.DeleteByStreamID("other")
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	n, err = repo.DeleteByStreamID("stream-1")
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	// hard delete: same stream can be re-ingested
	_, err = repo.UpsertByIdempotencyKey(validProviderHLSInit("init-again"))
	require.NoError(t, err)
}
