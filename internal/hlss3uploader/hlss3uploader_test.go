package hlss3uploader

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluenviron/mediamtx/internal/models"
)

type mockProvider struct {
	name        string
	uploadCount int32
	uploadErr   error
	delay       time.Duration
}

type readableMockProvider struct {
	mockProvider
	body          string
	contentType   string
	contentLength int64
	err           error
	ctxErr        error
}

func (m *mockProvider) Name() string {
	if m.name == "" {
		return "mock"
	}
	return m.name
}

func (m *mockProvider) UploadFile(ctx context.Context, localPath, remoteKey, contentType string) (string, error) {
	atomic.AddInt32(&m.uploadCount, 1)
	if m.delay > 0 {
		select {
		case <-time.After(m.delay):
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if m.uploadErr != nil {
		return "", m.uploadErr
	}
	return "mock-etag", nil
}

func (m *mockProvider) DeleteFolder(ctx context.Context, remoteKey string) error {
	return nil
}

func (m *mockProvider) Close() error {
	return nil
}

func (m *readableMockProvider) GetObject(ctx context.Context, key string) (io.ReadCloser, string, int64, error) {
	select {
	case <-ctx.Done():
		m.ctxErr = ctx.Err()
		return nil, "", -1, ctx.Err()
	default:
	}
	if m.err != nil {
		return nil, "", -1, m.err
	}
	return io.NopCloser(strings.NewReader(m.body)), m.contentType, m.contentLength, nil
}

type mockSegmentRepo struct {
	segments map[string]models.LiveHLSSegment
}

func (r *mockSegmentRepo) key(streamID, segmentName string) string {
	return streamID + "/" + segmentName
}
func (r *mockSegmentRepo) UpsertLocal(segment models.LiveHLSSegment) error {
	if r.segments == nil {
		r.segments = make(map[string]models.LiveHLSSegment)
	}
	segment.Status = models.LiveHLSSegmentStatusLocal
	r.segments[r.key(segment.StreamID, segment.SegmentName)] = segment
	return nil
}
func (r *mockSegmentRepo) UpsertUploading(segment models.LiveHLSSegment) error {
	return r.UpsertLocal(segment)
}
func (r *mockSegmentRepo) UpsertUploaded(segment models.LiveHLSSegment) error {
	if r.segments == nil {
		r.segments = make(map[string]models.LiveHLSSegment)
	}
	segment.Status = models.LiveHLSSegmentStatusUploadedS3
	r.segments[r.key(segment.StreamID, segment.SegmentName)] = segment
	return nil
}
func (r *mockSegmentRepo) UpsertUploadFailed(segment models.LiveHLSSegment) error {
	return r.UpsertLocal(segment)
}
func (r *mockSegmentRepo) MarkLocalDeleted(streamID, segmentName string, deletedAt time.Time) error {
	return nil
}
func (r *mockSegmentRepo) GetByStorageKey(storageKey string) (*models.LiveHLSSegment, error) {
	return nil, errors.New("not found")
}
func (r *mockSegmentRepo) GetByStreamAndSegment(streamID, segmentName string) (*models.LiveHLSSegment, error) {
	seg, ok := r.segments[r.key(streamID, segmentName)]
	if !ok {
		return nil, errors.New("not found")
	}
	return &seg, nil
}
func (r *mockSegmentRepo) ListWindow(streamID, sessionID string, since time.Time) ([]models.LiveHLSSegment, error) {
	return nil, nil
}
func (r *mockSegmentRepo) ListFMP4Window(streamID string, since time.Time) ([]models.LiveHLSSegment, error) {
	var out []models.LiveHLSSegment
	for _, seg := range r.segments {
		if seg.StreamID == streamID && !seg.StartedAt.Before(since) {
			out = append(out, seg)
		}
	}
	return out, nil
}
func (r *mockSegmentRepo) MaxSequence(streamID string) (int64, bool, error) { return 0, false, nil }

func TestHLSS3Uploader_InFlightAndCompletedExclusion(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "uploader-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	filePath := filepath.Join(tmpDir, "segment_001.m4s")
	if err := os.WriteFile(filePath, []byte("fake video content"), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	provider := &mockProvider{
		delay: 100 * time.Millisecond,
	}

	uploader := &HLSS3Uploader{
		Config: StorageConfig{
			Directory: tmpDir,
			Prefix:    "test-prefix",
		},
		provider: provider,
		ctx:      context.Background(),
	}

	expectedKey := "test-prefix/segment_001.m4s"

	// 1. Concurrent processFile calls for the same file
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			uploader.processFile(filePath)
		}()
	}
	wg.Wait()

	// Only 1 upload should have occurred due to in-flight exclusion
	if count := atomic.LoadInt32(&provider.uploadCount); count != 1 {
		t.Errorf("expected 1 upload call, got %d", count)
	}

	// Verify expectedKey is now in uploadedFiles
	if _, loaded := uploader.uploadedFiles.Load(expectedKey); !loaded {
		t.Errorf("expected key %q to be stored in uploadedFiles", expectedKey)
	}

	// Verify expectedKey is cleared from processingFiles
	if _, loaded := uploader.processingFiles.Load(expectedKey); loaded {
		t.Errorf("expected key %q to be cleared from processingFiles", expectedKey)
	}

	// 2. Subsequent processFile call should be skipped because of uploadedFiles
	uploader.processFile(filePath)
	if count := atomic.LoadInt32(&provider.uploadCount); count != 1 {
		t.Errorf("expected upload count to remain 1 after completed exclusion, got %d", count)
	}
}

func TestHLSS3Uploader_FailedUploadReleasesLockForRetry(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "uploader-fail-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	filePath := filepath.Join(tmpDir, "segment_002.m4s")
	if err := os.WriteFile(filePath, []byte("fake video content"), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	provider := &mockProvider{
		uploadErr: errors.New("network failure"),
	}

	uploader := &HLSS3Uploader{
		Config: StorageConfig{
			Directory: tmpDir,
			Prefix:    "test-prefix",
		},
		provider: provider,
		ctx:      context.Background(),
	}

	expectedKey := "test-prefix/segment_002.m4s"

	// 1. First attempt fails
	uploader.processFile(filePath)

	if count := atomic.LoadInt32(&provider.uploadCount); count != 1 {
		t.Fatalf("expected 1 upload attempt, got %d", count)
	}

	// Should NOT be in uploadedFiles
	if _, loaded := uploader.uploadedFiles.Load(expectedKey); loaded {
		t.Errorf("expected key %q NOT to be in uploadedFiles after failure", expectedKey)
	}

	// Should NOT be in processingFiles (lock released)
	if _, loaded := uploader.processingFiles.Load(expectedKey); loaded {
		t.Errorf("expected key %q to be cleared from processingFiles after failure", expectedKey)
	}

	// 2. Retry attempt succeeds after clearing error
	provider.uploadErr = nil
	uploader.processFile(filePath)

	if count := atomic.LoadInt32(&provider.uploadCount); count != 2 {
		t.Fatalf("expected 2 upload attempts after retry, got %d", count)
	}

	// Now should be in uploadedFiles
	if _, loaded := uploader.uploadedFiles.Load(expectedKey); !loaded {
		t.Errorf("expected key %q to be in uploadedFiles after retry success", expectedKey)
	}
}

func TestHLSS3UploaderProxyObjectUsesReadableProvider(t *testing.T) {
	provider := &readableMockProvider{
		body:          "segment-bytes",
		contentType:   "video/mp4",
		contentLength: int64(len("segment-bytes")),
	}
	uploader := &HLSS3Uploader{provider: provider}
	req := httptest.NewRequest(http.MethodGet, "/segment.mp4", nil)
	w := httptest.NewRecorder()

	if !uploader.ProxyObject(req.Context(), w, "live/cam/segment.mp4") {
		t.Fatalf("expected proxy success")
	}

	res := w.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status %d", res.StatusCode)
	}
	if got := res.Header.Get("Content-Type"); got != "video/mp4" {
		t.Fatalf("unexpected content type %q", got)
	}
	if got := res.Header.Get("Content-Length"); got != "13" {
		t.Fatalf("unexpected content length %q", got)
	}
	if got := res.Header.Get("Cache-Control"); got != "public, max-age=31536000, immutable" {
		t.Fatalf("unexpected cache-control %q", got)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "segment-bytes" {
		t.Fatalf("unexpected body %q", body)
	}
}

func TestHLSS3UploaderProxyObjectRequiresReadableProvider(t *testing.T) {
	uploader := &HLSS3Uploader{provider: &mockProvider{}}
	w := httptest.NewRecorder()

	if uploader.ProxyObject(context.Background(), w, "live/cam/segment.mp4") {
		t.Fatalf("expected proxy to be unsupported")
	}
	if w.Body.Len() != 0 || len(w.Header()) != 0 {
		t.Fatalf("unexpected response write for unsupported provider")
	}
}

func TestParseFMP4PlaylistAndMuxSessionWithUnderscores(t *testing.T) {
	tmpDir := t.TempDir()
	playlistPath := filepath.Join(tmpDir, "video1_stream.m3u8")
	playlist := "#EXTM3U\n" +
		"#EXT-X-VERSION:10\n" +
		"#EXT-X-MEDIA-SEQUENCE:42\n" +
		"#EXT-X-MAP:URI=\"prefix_with_under_video1_init.mp4\"\n" +
		"#EXT-X-PROGRAM-DATE-TIME:2026-08-20T12:00:00Z\n" +
		"#EXTINF:2.000,\n" +
		"prefix_with_under_video1_seg0.mp4\n"
	if err := os.WriteFile(playlistPath, []byte(playlist), 0644); err != nil {
		t.Fatal(err)
	}

	entries, initName := parseFMP4Playlist(playlistPath)
	if initName != "prefix_with_under_video1_init.mp4" {
		t.Fatalf("unexpected init name %q", initName)
	}
	if len(entries) != 1 || entries[0].MediaSequence != 42 || entries[0].DurationMS != 2000 {
		t.Fatalf("unexpected entries: %+v", entries)
	}
	if got := deriveMuxSessionID("prefix_with_under_video1_seg0.mp4", "video1"); got != "prefix_with_under" {
		t.Fatalf("unexpected mux session %q", got)
	}
	if got := deriveMuxSessionID("prefix_with_under_video1_init.mp4", "video1"); got != "prefix_with_under" {
		t.Fatalf("unexpected init mux session %q", got)
	}
}

func TestIngestFMP4PlaylistStoresMetadataForChildPath(t *testing.T) {
	tmpDir := t.TempDir()
	playlistPath := filepath.Join(tmpDir, "video1_stream.m3u8")
	segName := "mux_with_under_video1_seg0.mp4"
	initName := "mux_with_under_video1_init.mp4"
	if err := os.WriteFile(filepath.Join(tmpDir, segName), []byte("segment"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(playlistPath, []byte("#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:7\n#EXT-X-MAP:URI=\""+initName+"\"\n#EXT-X-PROGRAM-DATE-TIME:2026-08-20T12:00:00Z\n#EXTINF:2.500,\n"+segName+"\n"), 0644); err != nil {
		t.Fatal(err)
	}

	repo := &mockSegmentRepo{segments: make(map[string]models.LiveHLSSegment)}
	uploader := &HLSS3Uploader{
		Config:     StorageConfig{Directory: tmpDir, Prefix: "live-hls"},
		provider:   &mockProvider{},
		Repository: repo,
	}
	uploader.ingestFMP4Playlist(playlistPath, "base_path/with_under/video/720")

	seg := repo.segments["base_path/with_under/video/720/"+segName]
	if seg.DurationMS != 2500 || seg.MediaSequence == nil || *seg.MediaSequence != 7 {
		t.Fatalf("unexpected duration/media sequence: %+v", seg)
	}
	if seg.BaseStreamID != "base_path/with_under" || seg.TrackType != "video" || seg.Rendition != "720" {
		t.Fatalf("unexpected ABR metadata: %+v", seg)
	}
	if seg.MuxSessionID != "mux_with_under" || seg.InitSegmentName != initName || seg.PlaylistName != "video1_stream.m3u8" {
		t.Fatalf("unexpected fMP4 metadata: %+v", seg)
	}
	if seg.InitStorageKey != "live-hls/base_path/with_under/video/720/"+initName {
		t.Fatalf("unexpected init storage key %q", seg.InitStorageKey)
	}
}

func TestIngestFMP4PlaylistPreservesStartedAtWhenPDTMissing(t *testing.T) {
	tmpDir := t.TempDir()
	playlistPath := filepath.Join(tmpDir, "video1_stream.m3u8")
	segName := "mux_with_under_video1_seg0.mp4"
	initName := "mux_with_under_video1_init.mp4"
	existingStartedAt := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)

	if err := os.WriteFile(filepath.Join(tmpDir, segName), []byte("segment"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(playlistPath, []byte("#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:7\n#EXT-X-MAP:URI=\""+initName+"\"\n#EXTINF:2.500,\n"+segName+"\n"), 0644); err != nil {
		t.Fatal(err)
	}

	repo := &mockSegmentRepo{segments: map[string]models.LiveHLSSegment{
		"cam/video/720/" + segName: {
			StreamID:    "cam/video/720",
			SegmentName: segName,
			StartedAt:   existingStartedAt,
			Status:      models.LiveHLSSegmentStatusUploadedS3,
		},
	}}
	uploader := &HLSS3Uploader{
		Config:     StorageConfig{Directory: tmpDir, Prefix: "live-hls"},
		provider:   &mockProvider{},
		Repository: repo,
	}

	uploader.ingestFMP4Playlist(playlistPath, "cam/video/720")

	seg := repo.segments["cam/video/720/"+segName]
	if !seg.StartedAt.Equal(existingStartedAt) {
		t.Fatalf("expected existing StartedAt to be preserved, got %s", seg.StartedAt)
	}
	if seg.MediaSequence == nil || *seg.MediaSequence != 7 {
		t.Fatalf("expected playlist metadata to still be applied: %+v", seg)
	}
}

func TestIngestFMP4PlaylistEstimatesStartedAtFromModTimeWhenPDTMissing(t *testing.T) {
	tmpDir := t.TempDir()
	playlistPath := filepath.Join(tmpDir, "video1_stream.m3u8")
	segName := "mux_video1_seg0.mp4"
	initName := "mux_video1_init.mp4"
	modTime := time.Date(2026, 8, 20, 12, 0, 3, 0, time.UTC)

	segPath := filepath.Join(tmpDir, segName)
	if err := os.WriteFile(segPath, []byte("segment"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(segPath, modTime, modTime); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(playlistPath, []byte("#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:9\n#EXT-X-MAP:URI=\""+initName+"\"\n#EXTINF:3.000,\n"+segName+"\n"), 0644); err != nil {
		t.Fatal(err)
	}

	repo := &mockSegmentRepo{segments: make(map[string]models.LiveHLSSegment)}
	uploader := &HLSS3Uploader{
		Config:     StorageConfig{Directory: tmpDir, Prefix: "live-hls"},
		provider:   &mockProvider{},
		Repository: repo,
	}

	uploader.ingestFMP4Playlist(playlistPath, "cam/video/720")

	seg := repo.segments["cam/video/720/"+segName]
	expected := modTime.Add(-3 * time.Second)
	if !seg.StartedAt.Equal(expected) {
		t.Fatalf("expected StartedAt %s, got %s", expected, seg.StartedAt)
	}
	window, err := repo.ListFMP4Window("cam/video/720", expected.Add(-time.Second))
	if err != nil || len(window) != 1 {
		t.Fatalf("expected segment inside fMP4 window, len=%d err=%v", len(window), err)
	}
}
