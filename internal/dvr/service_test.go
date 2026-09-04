package dvr

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

type mockLinkProvider struct {
	link string
	err  error
}

func (p *mockLinkProvider) Name() string { return "link-provider" }
func (p *mockLinkProvider) UploadFile(ctx context.Context, localPath, remoteKey, contentType string) (string, error) {
	return "", nil
}
func (p *mockLinkProvider) DeleteFolder(ctx context.Context, prefix string) error { return nil }
func (p *mockLinkProvider) Close() error                                          { return nil }
func (p *mockLinkProvider) GetLink(ctx context.Context, key string) (string, error) {
	if p.err != nil {
		return "", p.err
	}
	return p.link, nil
}

func TestServeMediaRedirectsWhenLinkProviderPresent(t *testing.T) {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	repo := &fakeRepo{segments: []models.LiveHLSSegment{
		{
			StreamID:    "cam1",
			SegmentName: "seg10.ts",
			StorageKey:  "live-hls/cam1/seg10.ts",
			StorageETag: "00000000-0000-0000-0000-000000000002",
			StartedAt:   now,
		},
	}}
	expectedLink := "https://edge.aioz.network/download/00000000-0000-0000-0000-000000000002?ticket=signedToken"
	svc := &Service{
		Repository: repo,
		provider: &mockLinkProvider{
			link: expectedLink,
		},
	}
	req := httptest.NewRequest(http.MethodGet, "/media/cam1/seg10.ts", nil)
	w := httptest.NewRecorder()

	if !svc.ServeMedia(w, req, "cam1", "seg10.ts") {
		t.Fatalf("expected ServeMedia to return true for redirect")
	}

	res := w.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusFound {
		t.Fatalf("unexpected status %d, expected 302 StatusFound", res.StatusCode)
	}
	if got := res.Header.Get("Location"); got != expectedLink {
		t.Fatalf("unexpected Location header %q, expected %q", got, expectedLink)
	}
}

func TestDVRScrubbingAndSeekingFlow(t *testing.T) {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	tmpDir := t.TempDir()

	// Create local segment file on disk (recent segment)
	localSegPath := filepath.Join(tmpDir, "seg_live.mp4")
	if err := os.WriteFile(localSegPath, []byte("live-fmp4-bytes"), 0o644); err != nil {
		t.Fatal(err)
	}

	seq0 := int64(100)
	seq1 := int64(101)
	seqLive := int64(102)

	remoteUUID0 := "00000000-0000-0000-0000-000000000001"
	remoteUUID1 := "00000000-0000-0000-0000-000000000002"

	repo := &fakeRepo{segments: []models.LiveHLSSegment{
		{
			StreamID:    "cam1/video/720",
			SegmentName: "init.mp4",
			StorageKey:  "live-hls/cam1/video/720/init.mp4",
			StorageETag: remoteUUID0,
			StartedAt:   now.Add(-60 * time.Minute),
		},
		{
			StreamID:        "cam1/video/720",
			SegmentName:     "seg_45min_ago.mp4",
			StorageKey:      "live-hls/cam1/video/720/seg_45min_ago.mp4",
			StorageETag:     remoteUUID1,
			StartedAt:       now.Add(-45 * time.Minute),
			DurationMS:      2000,
			MediaSequence:   &seq0,
			InitSegmentName: "init.mp4",
			MuxSessionID:    "session_1",
		},
		{
			StreamID:        "cam1/video/720",
			SegmentName:     "seg_10min_ago.mp4",
			StorageKey:      "live-hls/cam1/video/720/seg_10min_ago.mp4",
			StorageETag:     remoteUUID1,
			StartedAt:       now.Add(-10 * time.Minute),
			DurationMS:      2000,
			MediaSequence:   &seq1,
			InitSegmentName: "init.mp4",
			MuxSessionID:    "session_1",
		},
		{
			StreamID:        "cam1/video/720",
			SegmentName:     "seg_live.mp4",
			LocalPath:       localSegPath,
			StartedAt:       now.Add(-2 * time.Second),
			DurationMS:      2000,
			MediaSequence:   &seqLive,
			InitSegmentName: "init.mp4",
			MuxSessionID:    "session_1",
		},
	}}

	expectedRemoteLink := "https://cdn.appdemo.cyou/download/00000000-0000-0000-0000-000000000002?ticket=validToken"
	svc := &Service{
		Repository: repo,
		provider: &mockLinkProvider{
			link: expectedRemoteLink,
		},
	}

	// 1. Test Playlist Rendering for live DVR
	playlist, ok, err := svc.RenderPlaylist("cam1/video/720", now)
	if err != nil || !ok {
		t.Fatalf("RenderPlaylist failed: ok=%v err=%v", ok, err)
	}
	plStr := string(playlist)

	// Must contain fMP4 Map
	if !strings.Contains(plStr, `#EXT-X-MAP:URI="/media/cam1/video/720/init.mp4"`) {
		t.Fatalf("playlist missing EXT-X-MAP:\n%s", plStr)
	}
	// Must contain media sequence
	if !strings.Contains(plStr, `#EXT-X-MEDIA-SEQUENCE:100`) {
		t.Fatalf("playlist missing media sequence:\n%s", plStr)
	}
	// Must contain both remote and local segments
	if !strings.Contains(plStr, `/media/cam1/video/720/seg_45min_ago.mp4`) {
		t.Fatalf("playlist missing 45min segment:\n%s", plStr)
	}
	if !strings.Contains(plStr, `/media/cam1/video/720/seg_live.mp4`) {
		t.Fatalf("playlist missing live segment:\n%s", plStr)
	}
	// Must NOT contain ENDLIST since stream is live
	if strings.Contains(plStr, `#EXT-X-ENDLIST`) {
		t.Fatalf("live DVR playlist must not contain EXT-X-ENDLIST:\n%s", plStr)
	}

	// 2. Test Seeking back to remote segment (45 mins ago) -> HTTP 302 Redirect
	reqRemote := httptest.NewRequest(http.MethodGet, "/media/cam1/video/720/seg_45min_ago.mp4", nil)
	wRemote := httptest.NewRecorder()
	if !svc.ServeMedia(wRemote, reqRemote, "cam1/video/720", "seg_45min_ago.mp4") {
		t.Fatalf("expected ServeMedia to handle remote segment seek")
	}
	resRemote := wRemote.Result()
	defer resRemote.Body.Close()
	if resRemote.StatusCode != http.StatusFound {
		t.Fatalf("seeking remote segment expected 302 Found, got %d", resRemote.StatusCode)
	}
	if loc := resRemote.Header.Get("Location"); loc != expectedRemoteLink {
		t.Fatalf("seeking remote segment expected Location %q, got %q", expectedRemoteLink, loc)
	}

	// 3. Test Seeking to live edge (recent local segment) -> HTTP 200 ServeFile
	reqLocal := httptest.NewRequest(http.MethodGet, "/media/cam1/video/720/seg_live.mp4", nil)
	wLocal := httptest.NewRecorder()
	if !svc.ServeMedia(wLocal, reqLocal, "cam1/video/720", "seg_live.mp4") {
		t.Fatalf("expected ServeMedia to handle local segment")
	}
	resLocal := wLocal.Result()
	defer resLocal.Body.Close()
	if resLocal.StatusCode != http.StatusOK {
		t.Fatalf("local segment expected 200 OK, got %d", resLocal.StatusCode)
	}
	localBody, _ := io.ReadAll(resLocal.Body)
	if string(localBody) != "live-fmp4-bytes" {
		t.Fatalf("local segment content mismatch, got %q", string(localBody))
	}
}

func TestRenderPlaylistSlidingWindowSegmentCount(t *testing.T) {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	initSeg := "stream1_init.mp4"
	var segs []models.LiveHLSSegment
	// 20 segments from seq 100 to 119
	for i := 0; i < 20; i++ {
		seq := int64(100 + i)
		segs = append(segs, models.LiveHLSSegment{
			StreamID:        "stream1/video/720",
			SegmentName:     fmt.Sprintf("seg%02d.mp4", i),
			StartedAt:       now.Add(time.Duration(i*2) * time.Second),
			DurationMS:      2000,
			MediaSequence:   &seq,
			InitSegmentName: initSeg,
			MuxSessionID:    "session1",
		})
	}

	repo := &fakeRepo{segments: segs}

	// Test 1: SegmentCount = 7 (sliding window of 7 segments)
	svc := &Service{
		Repository:   repo,
		SegmentCount: 7,
	}

	playlist, ok, err := svc.RenderPlaylist("stream1/video/720", now.Add(40*time.Second))
	if err != nil || !ok {
		t.Fatalf("expected playlist, ok=%v err=%v", ok, err)
	}
	body := string(playlist)

	// Must contain MEDIA-SEQUENCE of the 13th segment (100 + 13 = 113)
	if !strings.Contains(body, "#EXT-X-MEDIA-SEQUENCE:113") {
		t.Fatalf("expected #EXT-X-MEDIA-SEQUENCE:113, got:\n%s", body)
	}

	// Must contain init map
	if !strings.Contains(body, `#EXT-X-MAP:URI="/media/stream1/video/720/stream1_init.mp4"`) {
		t.Fatalf("missing EXT-X-MAP tag:\n%s", body)
	}

	// Must contain exactly 7 segments (seg13.mp4 to seg19.mp4)
	for i := 13; i < 20; i++ {
		expectedSeg := fmt.Sprintf("seg%02d.mp4", i)
		if !strings.Contains(body, expectedSeg) {
			t.Fatalf("expected sliding window to contain %s, got:\n%s", expectedSeg, body)
		}
	}

	// Must NOT contain older segments (seg00.mp4 to seg12.mp4)
	for i := 0; i < 13; i++ {
		unexpectedSeg := fmt.Sprintf("seg%02d.mp4", i)
		if strings.Contains(body, unexpectedSeg) {
			t.Fatalf("expected sliding window to exclude older %s, got:\n%s", unexpectedSeg, body)
		}
	}

	// Count occurrences of #EXTINF
	extinfCount := strings.Count(body, "#EXTINF:")
	if extinfCount != 7 {
		t.Fatalf("expected 7 #EXTINF entries, got %d:\n%s", extinfCount, body)
	}

	// Test 2: SegmentCount = 0 (unlimited window, all 20 segments)
	svcUnlimited := &Service{
		Repository:   repo,
		SegmentCount: 0,
	}
	plUnlimited, ok, err := svcUnlimited.RenderPlaylist("stream1/video/720", now.Add(40*time.Second))
	if err != nil || !ok {
		t.Fatalf("expected unlimited playlist, ok=%v err=%v", ok, err)
	}
	if strings.Count(string(plUnlimited), "#EXTINF:") != 20 {
		t.Fatalf("expected 20 #EXTINF entries when SegmentCount=0, got %d", strings.Count(string(plUnlimited), "#EXTINF:"))
	}
}

