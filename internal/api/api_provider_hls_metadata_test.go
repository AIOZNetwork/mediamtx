package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bluenviron/mediamtx/internal/models"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type providerHLSTestRepo struct {
	metadata models.ProviderHLSMetadata
	err      error
}

func (r *providerHLSTestRepo) UpsertByIdempotencyKey(metadata models.ProviderHLSMetadata) (*models.ProviderHLSMetadata, error) {
	r.metadata = metadata
	if r.err != nil {
		return nil, r.err
	}
	return &metadata, nil
}

func (r *providerHLSTestRepo) ListWindow(models.ProviderHLSMetadataWindowQuery) ([]models.ProviderHLSMetadata, error) {
	return nil, nil
}

func TestProviderHLSMetadataNestedAPIContract(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := &providerHLSTestRepo{}
	a := &API{ProviderHLSMetadataRepository: repo, Parent: &testParent{}}
	publishedAt := time.Unix(200, 0).UTC()
	programDateTime := time.Unix(201, 0).UTC()
	seq := int64(12)
	body := map[string]any{
		"event_id":      "event-1",
		"stream_id":     "stream-1",
		"mediamtx_path": "live/stream-1",
		"job_id":        "job-1",
		"generation":    2,
		"rendition": map[string]any{
			"name":      "main",
			"width":     1280,
			"height":    720,
			"bandwidth": 2500000,
			"codecs":    "avc1.64001f,mp4a.40.2",
		},
		"object": map[string]any{
			"kind":         "segment",
			"name":         "seg-12.m4s",
			"provider":     "depin",
			"contract_id":  "contract-1",
			"storage_key":  "streams/stream-1/seg-12.m4s",
			"url":          "https://example.test/seg-12.m4s",
			"object_id":    "object-12",
			"content_type": "video/iso.segment",
			"size_bytes":   1234,
		},
		"hls": map[string]any{
			"media_sequence":    seq,
			"duration_ms":       2000,
			"program_date_time": programDateTime,
			"init_segment_name": "init.mp4",
			"init_storage_key":  "streams/stream-1/init.mp4",
		},
		"published_at": publishedAt,
	}

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	buf, err := json.Marshal(body)
	require.NoError(t, err)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v3/internal/provider-hls/metadata", bytes.NewReader(buf))
	ctx.Request.Header.Set("Content-Type", "application/json")
	ctx.Request.Header.Set("Idempotency-Key", "idem-header")

	a.onProviderHLSMetadataIngest(ctx)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "idem-header", repo.metadata.IdempotencyKey)
	require.Equal(t, "event-1", repo.metadata.EventID)
	require.Equal(t, "live/stream-1", repo.metadata.StreamID)
	require.Equal(t, "live/stream-1", repo.metadata.MediaMTXPath)
	require.Equal(t, int64(2), repo.metadata.Generation)
	require.Equal(t, "main", repo.metadata.RenditionName)
	require.Equal(t, int64(2500000), repo.metadata.Bandwidth)
	require.Equal(t, "seg-12.m4s", repo.metadata.ObjectName)
	require.Equal(t, "depin", repo.metadata.Provider)
	require.Equal(t, "video/iso.segment", repo.metadata.ContentType)
	require.Equal(t, seq, *repo.metadata.MediaSequence)
	require.Equal(t, int64(2000), repo.metadata.DurationMS)
	require.Equal(t, "init.mp4", repo.metadata.InitSegmentName)
	require.True(t, publishedAt.Equal(repo.metadata.PublishedAt))
	require.True(t, programDateTime.Equal(*repo.metadata.ProgramDateTime))
}

func TestProviderHLSMetadataAPIRequiresIdempotencyHeader(t *testing.T) {
	gin.SetMode(gin.TestMode)
	a := &API{ProviderHLSMetadataRepository: &providerHLSTestRepo{}, Parent: &testParent{}}
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v3/internal/provider-hls/metadata", bytes.NewReader([]byte(`{}`)))
	ctx.Request.Header.Set("Content-Type", "application/json")

	a.onProviderHLSMetadataIngest(ctx)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}
