package api //nolint:revive

import (
	"bytes"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/models"
	"github.com/bluenviron/mediamtx/internal/test"
)

type fakeLiveSegmentRepo struct {
	models.LiveHLSSegmentRepository
	segments map[string]models.LiveHLSSegment
	calls    int
}

func (r *fakeLiveSegmentRepo) UpsertUploaded(s models.LiveHLSSegment) error {
	r.calls++
	r.segments[s.StreamID+"|"+s.SegmentName] = s
	return nil
}

func TestInternalLiveSegments(t *testing.T) {
	repo := &fakeLiveSegmentRepo{segments: map[string]models.LiveHLSSegment{}}
	api := API{
		Address:      "localhost:9997",
		ReadTimeout:  conf.Duration(10 * time.Second),
		WriteTimeout: conf.Duration(10 * time.Second),
		AuthManager:  test.NilAuthManager,
		LiveSegments: repo,
		Parent:       &testParent{},
	}
	require.NoError(t, api.Initialize())
	defer api.Close()

	hc := &http.Client{Transport: &http.Transport{}}
	post := func(body string) int {
		res, err := hc.Post("http://localhost:9997/v3/internal/live-segments", "application/json", bytes.NewBufferString(body))
		require.NoError(t, err)
		defer res.Body.Close()
		return res.StatusCode
	}

	valid := `{"stream_id":"a/b","base_stream_id":"a","rendition":"720","track_type":"video","segment_name":"seg1.m4s",
	"mux_session_id":"g1","storage_backend":"s3","storage_key":"a/b/720/g1/seg1.m4s","started_at":"2024-01-02T03:04:05Z",
	"duration_ms":2000,"size_bytes":1234,"sequence":7}`

	require.Equal(t, http.StatusNoContent, post(valid))
	require.Equal(t, http.StatusNoContent, post(valid))
	require.Equal(t, 2, repo.calls)
	require.Len(t, repo.segments, 1)

	got := repo.segments["a/b|seg1.m4s"]
	require.Equal(t, "a", got.BaseStreamID)
	require.Equal(t, "720", got.Rendition)
	require.Equal(t, "video", got.TrackType)
	require.Equal(t, "g1", got.MuxSessionID)
	require.Equal(t, "s3", got.StorageBackend)
	require.Equal(t, int64(2000), got.DurationMS)
	require.Equal(t, int64(1234), got.SizeBytes)
	require.Equal(t, int64(7), *got.Sequence)
	require.Equal(t, testTime, got.StartedAt)
	require.NotNil(t, got.UploadedAt)

	require.Equal(t, http.StatusBadRequest, post(`not json`))
	require.Equal(t, http.StatusBadRequest, post(`{"stream_id":"a","segment_name":"s","rendition":"720","duration_ms":0,"started_at":"2024-01-02T03:04:05Z"}`))
	require.Equal(t, http.StatusBadRequest, post(`{"stream_id":"a","segment_name":"s","rendition":"720","duration_ms":10}`))
	require.Equal(t, http.StatusBadRequest, post(`{"segment_name":"s","rendition":"720","duration_ms":10,"started_at":"2024-01-02T03:04:05Z"}`))
	require.Equal(t, 2, repo.calls)
}
