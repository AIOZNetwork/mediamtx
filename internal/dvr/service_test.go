package dvr

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bluenviron/mediamtx/internal/models"
)

type fakeRepo struct {
	segments []models.LiveHLSSegment
}

type readableProvider struct {
	body          string
	contentType   string
	contentLength int64
	err           error
}

func (r *fakeRepo) UpsertLocal(segment models.LiveHLSSegment) error        { return nil }
func (r *fakeRepo) UpsertUploading(segment models.LiveHLSSegment) error    { return nil }
func (r *fakeRepo) UpsertUploaded(segment models.LiveHLSSegment) error     { return nil }
func (r *fakeRepo) UpsertUploadFailed(segment models.LiveHLSSegment) error { return nil }
func (r *fakeRepo) MarkLocalDeleted(streamID, segmentName string, deletedAt time.Time) error {
	return nil
}
func (r *fakeRepo) GetByStorageKey(storageKey string) (*models.LiveHLSSegment, error) {
	return nil, nil
}
func (r *fakeRepo) GetByStreamAndSegment(streamID, segmentName string) (*models.LiveHLSSegment, error) {
	for _, seg := range r.segments {
		if seg.StreamID == streamID && seg.SegmentName == segmentName {
			return &seg, nil
		}
	}
	return nil, nil
}
func (r *fakeRepo) MaxSequence(streamID string) (int64, bool, error) { return 0, false, nil }
func (r *fakeRepo) ListWindow(streamID, sessionID string, since time.Time) ([]models.LiveHLSSegment, error) {
	var out []models.LiveHLSSegment
	for _, seg := range r.segments {
		if seg.StreamID == streamID && (sessionID == "" || seg.SessionID == sessionID) && !seg.StartedAt.Before(since) {
			out = append(out, seg)
		}
	}
	return out, nil
}

func (r *fakeRepo) ListFMP4Window(streamID string, since time.Time) ([]models.LiveHLSSegment, error) {
	var out []models.LiveHLSSegment
	for _, seg := range r.segments {
		if seg.StreamID == streamID && !seg.StartedAt.Before(since) {
			out = append(out, seg)
		}
	}
	return out, nil
}

func (p *readableProvider) Name() string { return "readable" }
func (p *readableProvider) UploadFile(ctx context.Context, localPath, remoteKey, contentType string) (string, error) {
	return "", nil
}
func (p *readableProvider) DeleteFolder(ctx context.Context, prefix string) error { return nil }
func (p *readableProvider) Close() error                                          { return nil }
func (p *readableProvider) GetObject(ctx context.Context, key string) (io.ReadCloser, string, int64, error) {
	if p.err != nil {
		return nil, "", -1, p.err
	}
	return io.NopCloser(strings.NewReader(p.body)), p.contentType, p.contentLength, nil
}

func TestRenderPlaylistRollingWindowAndMediaSequence(t *testing.T) {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	seqOld := int64(9)
	seq10 := int64(10)
	seq11 := int64(11)
	sessionID := "cam1-session"
	repo := &fakeRepo{segments: []models.LiveHLSSegment{
		{StreamID: "cam1", SessionID: sessionID, SegmentName: "old.ts", StartedAt: now.Add(-Window - time.Second), DurationMS: 4000, Sequence: &seqOld},
		{StreamID: "cam1", SessionID: sessionID, SegmentName: "seg10.ts", StartedAt: now.Add(-10 * time.Second), DurationMS: 4013, Sequence: &seq10},
		{StreamID: "cam1", SessionID: sessionID, SegmentName: "seg11.ts", StartedAt: now.Add(-6 * time.Second), DurationMS: 3992, Sequence: &seq11},
		{StreamID: "cam1", SessionID: "previous-session", SegmentName: "previous.ts", StartedAt: now.Add(-4 * time.Second), DurationMS: 3992},
	}}

	svc := &Service{Repository: repo}
	svc.sessions = map[string]string{"cam1": sessionID}
	svc.active = map[string]bool{"cam1": true}
	playlist, ok, err := svc.RenderPlaylist("cam1", now)
	if err != nil || !ok {
		t.Fatalf("expected playlist, ok=%v err=%v", ok, err)
	}
	body := string(playlist)
	for _, expected := range []string{
		"#EXT-X-TARGETDURATION:5",
		"#EXT-X-MEDIA-SEQUENCE:10",
		"#EXT-X-PROGRAM-DATE-TIME:2026-08-20T11:59:50Z",
		"#EXTINF:4.013,\n/media/cam1/seg10.ts",
		"#EXTINF:3.992,\n/media/cam1/seg11.ts",
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("playlist missing %q:\n%s", expected, body)
		}
	}
	if strings.Contains(body, "old.ts") {
		t.Fatalf("playlist included segment outside window:\n%s", body)
	}
	if strings.Contains(body, "previous.ts") {
		t.Fatalf("playlist included segment from another session:\n%s", body)
	}
	if strings.Contains(body, "#EXT-X-ENDLIST") {
		t.Fatalf("active playlist must not include ENDLIST:\n%s", body)
	}
}

func TestRenderFMP4PlaylistWithMap(t *testing.T) {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	seq := int64(100)
	repo := &fakeRepo{segments: []models.LiveHLSSegment{
		{
			StreamID: "cam1/video/720", SegmentName: "mux_with_under_video1_init.mp4", StartedAt: now.Add(-9 * time.Second),
		},
		{
			StreamID: "cam1/video/720", SegmentName: "mux_with_under_video1_seg0.mp4", StartedAt: now.Add(-8 * time.Second),
			DurationMS: 2000, MediaSequence: &seq, InitSegmentName: "mux_with_under_video1_init.mp4", MuxSessionID: "mux_with_under",
		},
	}}
	svc := &Service{Repository: repo}
	playlist, ok, err := svc.RenderPlaylist("cam1/video/720", now)
	if err != nil || !ok {
		t.Fatalf("expected fMP4 playlist, ok=%v err=%v", ok, err)
	}
	body := string(playlist)
	for _, expected := range []string{
		"#EXT-X-VERSION:10",
		"#EXT-X-MEDIA-SEQUENCE:100",
		"#EXT-X-MAP:URI=\"/media/cam1/video/720/mux_with_under_video1_init.mp4\"",
		"#EXT-X-PROGRAM-DATE-TIME:2026-08-20T11:59:52Z",
		"#EXTINF:2.000,\n/media/cam1/video/720/mux_with_under_video1_seg0.mp4",
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("playlist missing %q:\n%s", expected, body)
		}
	}
	if strings.Contains(body, "#EXT-X-ENDLIST") {
		t.Fatalf("fMP4 DVR playlist must not endlist by default:\n%s", body)
	}
}

func TestRenderFMP4PlaylistDiscontinuityOnMuxSessionAndInitChange(t *testing.T) {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	seq1 := int64(10)
	seq2 := int64(11)
	repo := &fakeRepo{segments: []models.LiveHLSSegment{
		{StreamID: "cam1/video/720", SegmentName: "muxA_video1_seg0.mp4", StartedAt: now.Add(-4 * time.Second), DurationMS: 2000, MediaSequence: &seq1, InitSegmentName: "muxA_video1_init.mp4", MuxSessionID: "muxA"},
		{StreamID: "cam1/video/720", SegmentName: "muxB_video1_seg0.mp4", StartedAt: now.Add(-2 * time.Second), DurationMS: 2000, MediaSequence: &seq2, InitSegmentName: "muxB_video1_init.mp4", MuxSessionID: "muxB"},
	}}
	svc := &Service{Repository: repo}
	playlist, ok, err := svc.RenderPlaylist("cam1/video/720", now)
	if err != nil || !ok {
		t.Fatalf("expected fMP4 playlist, ok=%v err=%v", ok, err)
	}
	body := string(playlist)
	if strings.Count(body, "#EXT-X-MAP") != 2 {
		t.Fatalf("expected two EXT-X-MAP tags:\n%s", body)
	}
	if !strings.Contains(body, "#EXT-X-DISCONTINUITY\n#EXT-X-MAP:URI=\"/media/cam1/video/720/muxB_video1_init.mp4\"") {
		t.Fatalf("expected discontinuity before changed init map:\n%s", body)
	}
}

func TestServeMediaProxiesRemoteReadableProvider(t *testing.T) {
	repo := &fakeRepo{segments: []models.LiveHLSSegment{
		{
			StreamID:    "cam1",
			SegmentName: "seg10.ts",
			StorageKey:  "live-hls/cam1/seg10.ts",
		},
	}}
	svc := &Service{
		Repository: repo,
		provider: &readableProvider{
			body:          "ts-bytes",
			contentType:   "video/mp2t",
			contentLength: int64(len("ts-bytes")),
		},
	}
	req := httptest.NewRequest(http.MethodGet, "/media/cam1/seg10.ts", nil)
	w := httptest.NewRecorder()

	if !svc.ServeMedia(w, req, "cam1", "seg10.ts") {
		t.Fatalf("expected ServeMedia to proxy remote object")
	}

	res := w.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status %d", res.StatusCode)
	}
	if got := res.Header.Get("Content-Type"); got != "video/mp2t" {
		t.Fatalf("unexpected content type %q", got)
	}
	if got := res.Header.Get("Content-Length"); got != "8" {
		t.Fatalf("unexpected content length %q", got)
	}
	if got := res.Header.Get("Cache-Control"); got != "public, max-age=31536000, immutable" {
		t.Fatalf("unexpected cache control %q", got)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "ts-bytes" {
		t.Fatalf("unexpected body %q", body)
	}
}
