package dvr

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bluenviron/mediamtx/internal/hlss3uploader"
	"github.com/bluenviron/mediamtx/internal/models"
)

type ReadyMSCall struct {
	StreamID    string
	SegmentName string
	MS          int64
}

type ServeCountCall struct {
	StreamID    string
	SegmentName string
	Outcome     string
	Counts      [4]int64
}

type fakeRepo struct {
	mu              sync.Mutex
	segments        []models.LiveHLSSegment
	ReadyMSCalls    []ReadyMSCall
	ServeCountCalls []ServeCountCall
	incServeErr     error
	incServeHook    func(outcome string)
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
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, seg := range r.segments {
		if seg.StreamID == streamID && seg.SegmentName == segmentName {
			cp := seg
			return &cp, nil
		}
	}
	return nil, nil
}
func (r *fakeRepo) MaxSequence(streamID string) (int64, bool, error) { return 0, false, nil }
func (r *fakeRepo) ListUploadedObjects(_, _, _ string) ([]models.LiveHLSSegment, error) {
	return nil, nil
}
func (r *fakeRepo) ListWindow(streamID, sessionID string, since time.Time) ([]models.LiveHLSSegment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []models.LiveHLSSegment
	for _, seg := range r.segments {
		if seg.StreamID == streamID && (sessionID == "" || seg.SessionID == sessionID) && !seg.StartedAt.Before(since) {
			out = append(out, seg)
		}
	}
	return out, nil
}

func (r *fakeRepo) ListFMP4Window(streamID string, since time.Time) ([]models.LiveHLSSegment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []models.LiveHLSSegment
	for _, seg := range r.segments {
		if seg.StreamID == streamID && !seg.StartedAt.Before(since) {
			out = append(out, seg)
		}
	}
	return out, nil
}

func (r *fakeRepo) SetReadyMS(ctx context.Context, streamID, segmentName string, ms int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ReadyMSCalls = append(r.ReadyMSCalls, ReadyMSCall{
		StreamID:    streamID,
		SegmentName: segmentName,
		MS:          ms,
	})
	for i := range r.segments {
		if r.segments[i].StreamID == streamID && r.segments[i].SegmentName == segmentName {
			r.segments[i].ReadyMS = &ms
			break
		}
	}
	return nil
}

func (r *fakeRepo) AddServeCounts(ctx context.Context, streamID, segmentName string, local, redirect, proxy, miss int64) error {
	if r.incServeHook != nil {
		r.incServeHook("batch")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	outcome := ""
	switch {
	case local != 0:
		outcome = serveOutcomeLocal
	case redirect != 0:
		outcome = serveOutcomeRedirect
	case proxy != 0:
		outcome = serveOutcomeProxy
	case miss != 0:
		outcome = serveOutcomeMiss
	}
	r.ServeCountCalls = append(r.ServeCountCalls, ServeCountCall{
		StreamID:    streamID,
		SegmentName: segmentName,
		Outcome:     outcome,
		Counts:      [4]int64{local, redirect, proxy, miss},
	})
	if r.incServeErr != nil {
		return r.incServeErr
	}
	for i := range r.segments {
		if r.segments[i].StreamID == streamID && r.segments[i].SegmentName == segmentName {
			r.segments[i].LocalCount += local
			r.segments[i].RedirectCount += redirect
			r.segments[i].ProxyCount += proxy
			r.segments[i].MissCount += miss
			break
		}
	}
	return nil
}

func (r *fakeRepo) getServeCountCalls() []ServeCountCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]ServeCountCall, len(r.ServeCountCalls))
	copy(cp, r.ServeCountCalls)
	return cp
}

func (r *fakeRepo) waitForServeCountCalls(expected int, timeout time.Duration) []ServeCountCall {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		if len(r.ServeCountCalls) >= expected {
			cp := make([]ServeCountCall, len(r.ServeCountCalls))
			copy(cp, r.ServeCountCalls)
			r.mu.Unlock()
			return cp
		}
		r.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]ServeCountCall, len(r.ServeCountCalls))
	copy(cp, r.ServeCountCalls)
	return cp
}

func (r *fakeRepo) getSegment(streamID, segmentName string) *models.LiveHLSSegment {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, seg := range r.segments {
		if seg.StreamID == streamID && seg.SegmentName == segmentName {
			cp := seg
			return &cp
		}
	}
	return nil
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
	svc.EndSession("cam1")

	calls := repo.waitForServeCountCalls(1, time.Second)
	if len(calls) != 1 {
		t.Fatalf("expected 1 ServeCountCall, got %d", len(calls))
	}
	if call := calls[0]; call.StreamID != "cam1" || call.SegmentName != "seg10.ts" || call.Outcome != "proxy" {
		t.Fatalf("unexpected ServeCountCall: %+v", call)
	}
	seg := repo.getSegment("cam1", "seg10.ts")
	if seg == nil || seg.ProxyCount != 1 {
		t.Fatalf("expected ProxyCount 1, got %v", seg)
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

type fallbackMockProvider struct {
	link          string
	linkErr       error
	body          string
	contentType   string
	contentLength int64
	readErr       error
}

func (p *fallbackMockProvider) Name() string { return "fallback-provider" }
func (p *fallbackMockProvider) UploadFile(ctx context.Context, localPath, remoteKey, contentType string) (string, error) {
	return "", nil
}
func (p *fallbackMockProvider) DeleteFolder(ctx context.Context, prefix string) error { return nil }
func (p *fallbackMockProvider) Close() error                                          { return nil }
func (p *fallbackMockProvider) GetLink(ctx context.Context, key string) (string, error) {
	if p.linkErr != nil {
		return "", p.linkErr
	}
	return p.link, nil
}
func (p *fallbackMockProvider) GetObject(ctx context.Context, key string) (io.ReadCloser, string, int64, error) {
	if p.readErr != nil {
		return nil, "", -1, p.readErr
	}
	return io.NopCloser(strings.NewReader(p.body)), p.contentType, p.contentLength, nil
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
	svc.EndSession("cam1")

	calls := repo.waitForServeCountCalls(1, time.Second)
	if len(calls) != 1 {
		t.Fatalf("expected 1 ServeCountCall, got %d", len(calls))
	}
	if call := calls[0]; call.StreamID != "cam1" || call.SegmentName != "seg10.ts" || call.Outcome != "redirect" {
		t.Fatalf("unexpected ServeCountCall: %+v", call)
	}
	seg := repo.getSegment("cam1", "seg10.ts")
	if seg == nil || seg.RedirectCount != 1 {
		t.Fatalf("expected RedirectCount 1, got %v", seg)
	}
}

func TestServeMediaLocal(t *testing.T) {
	tmpDir := t.TempDir()
	localFile := filepath.Join(tmpDir, "seg10.ts")
	if err := os.WriteFile(localFile, []byte("ts-local-bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	repo := &fakeRepo{segments: []models.LiveHLSSegment{
		{
			StreamID:    "cam1",
			SegmentName: "seg10.ts",
			LocalPath:   localFile,
		},
	}}
	svc := &Service{Repository: repo}
	req := httptest.NewRequest(http.MethodGet, "/media/cam1/seg10.ts", nil)
	w := httptest.NewRecorder()

	if !svc.ServeMedia(w, req, "cam1", "seg10.ts") {
		t.Fatalf("expected ServeMedia to serve local file")
	}
	res := w.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status %d", res.StatusCode)
	}
	body, _ := io.ReadAll(res.Body)
	if string(body) != "ts-local-bytes" {
		t.Fatalf("unexpected body %q", body)
	}
	svc.EndSession("cam1")
	calls := repo.waitForServeCountCalls(1, time.Second)
	if len(calls) != 1 {
		t.Fatalf("expected 1 ServeCountCall, got %d", len(calls))
	}
	if call := calls[0]; call.StreamID != "cam1" || call.SegmentName != "seg10.ts" || call.Outcome != "local" {
		t.Fatalf("unexpected ServeCountCall: %+v", call)
	}
	seg := repo.getSegment("cam1", "seg10.ts")
	if seg == nil || seg.LocalCount != 1 {
		t.Fatalf("expected LocalCount 1, got %v", seg)
	}
}

func TestServeMediaMiss(t *testing.T) {
	repo := &fakeRepo{segments: []models.LiveHLSSegment{
		{
			StreamID:    "cam1",
			SegmentName: "missing.ts",
			StorageKey:  "live-hls/cam1/missing.ts",
		},
	}}
	svc := &Service{Repository: repo}
	req := httptest.NewRequest(http.MethodGet, "/media/cam1/missing.ts", nil)
	w := httptest.NewRecorder()

	if svc.ServeMedia(w, req, "cam1", "missing.ts") {
		t.Fatalf("expected ServeMedia to return false on miss")
	}
	svc.EndSession("cam1")
	calls := repo.waitForServeCountCalls(1, time.Second)
	if len(calls) != 1 {
		t.Fatalf("expected 1 ServeCountCall, got %d", len(calls))
	}
	if call := calls[0]; call.StreamID != "cam1" || call.SegmentName != "missing.ts" || call.Outcome != "miss" {
		t.Fatalf("unexpected ServeCountCall: %+v", call)
	}
	seg := repo.getSegment("cam1", "missing.ts")
	if seg == nil || seg.MissCount != 1 {
		t.Fatalf("expected MissCount 1, got %v", seg)
	}
}

func TestServeMediaNilSegmentSkipsCounting(t *testing.T) {
	repo := &fakeRepo{}
	svc := &Service{Repository: repo}
	req := httptest.NewRequest(http.MethodGet, "/media/cam1/unknown.ts", nil)
	w := httptest.NewRecorder()

	if svc.ServeMedia(w, req, "cam1", "unknown.ts") {
		t.Fatalf("expected ServeMedia to return false on unknown segment")
	}
	time.Sleep(20 * time.Millisecond)
	calls := repo.getServeCountCalls()
	if len(calls) != 0 {
		t.Fatalf("expected 0 ServeCountCalls for nil segment, got %d", len(calls))
	}
}

func TestServeMediaRepoErrorResultUnchanged(t *testing.T) {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	repo := &fakeRepo{
		incServeErr: fmt.Errorf("db failure"),
		segments: []models.LiveHLSSegment{
			{
				StreamID:    "cam1",
				SegmentName: "seg10.ts",
				StorageKey:  "live-hls/cam1/seg10.ts",
				StartedAt:   now,
			},
		},
	}
	expectedLink := "https://edge.aioz.network/download/seg10.ts"
	svc := &Service{
		Repository: repo,
		provider: &mockLinkProvider{
			link: expectedLink,
		},
	}
	req := httptest.NewRequest(http.MethodGet, "/media/cam1/seg10.ts", nil)
	w := httptest.NewRecorder()

	if !svc.ServeMedia(w, req, "cam1", "seg10.ts") {
		t.Fatalf("expected ServeMedia to succeed despite repo error")
	}
	res := w.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusFound {
		t.Fatalf("unexpected status %d, expected 302 StatusFound", res.StatusCode)
	}
	if got := res.Header.Get("Location"); got != expectedLink {
		t.Fatalf("unexpected Location %q, expected %q", got, expectedLink)
	}
	svc.EndSession("cam1")
	calls := repo.waitForServeCountCalls(1, time.Second)
	if len(calls) != 1 {
		t.Fatalf("expected 1 ServeCountCall attempt even if repo errored, got %d", len(calls))
	}
}

func TestServeMediaBlockedRepoDoesNotDelayResponse(t *testing.T) {
	localFile := filepath.Join(t.TempDir(), "seg.ts")
	if err := os.WriteFile(localFile, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}, 1), make(chan struct{})
	repo := &fakeRepo{segments: []models.LiveHLSSegment{{StreamID: "cam1", SegmentName: "seg.ts", LocalPath: localFile}}, incServeHook: func(string) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
	}}
	svc := &Service{Repository: repo}
	serve := func() {
		if !svc.ServeMedia(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), "cam1", "seg.ts") {
			t.Error("serve failed")
		}
	}
	serve()
	done := make(chan struct{})
	go func() { svc.flushServeCounts(""); close(done) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("flush did not start")
	}
	returned := make(chan struct{})
	go func() { serve(); close(returned) }()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("request blocked on DB flush")
	}
	close(release)
	<-done
	svc.Close()
	if got := repo.getSegment("cam1", "seg.ts").LocalCount; got != 2 {
		t.Fatalf("lost concurrent increment: %d", got)
	}
}

func TestServeCountsPeriodicFlush(t *testing.T) {
	repo := &fakeRepo{segments: []models.LiveHLSSegment{{StreamID: "cam1", SegmentName: "missing.ts"}}}
	svc := &Service{Repository: repo}
	svc.startServeCountFlush(10 * time.Millisecond)
	defer svc.Close()
	svc.ServeMedia(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), "cam1", "missing.ts")
	calls := repo.waitForServeCountCalls(1, time.Second)
	if len(calls) != 1 || calls[0].Counts != [4]int64{0, 0, 0, 1} {
		t.Fatalf("periodic flush: %+v", calls)
	}
}

func TestServeCountsRetryAndClose(t *testing.T) {
	repo := &fakeRepo{segments: []models.LiveHLSSegment{{StreamID: "cam1", SegmentName: "a.ts"}}, incServeErr: fmt.Errorf("db failure")}
	svc := &Service{Repository: repo}
	svc.incServeCount(&repo.segments[0], "cam1", "a.ts", "local")
	svc.EndSession("cam1")
	svc.incServeCount(&repo.segments[0], "cam1", "a.ts", "proxy")
	repo.mu.Lock()
	repo.incServeErr = nil
	repo.mu.Unlock()
	svc.Close()
	seg := repo.getSegment("cam1", "a.ts")
	if seg.LocalCount != 1 || seg.ProxyCount != 1 {
		t.Fatalf("lost retry or close flush: %+v", seg)
	}
}

func TestServeCountsConcurrentAggregation(t *testing.T) {
	repo := &fakeRepo{segments: []models.LiveHLSSegment{{StreamID: "cam1", SegmentName: "a.ts"}}}
	svc := &Service{Repository: repo}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			svc.incServeCount(&repo.segments[0], "cam1", "a.ts", "local")
		}()
	}
	wg.Wait()
	if len(repo.getServeCountCalls()) != 0 {
		t.Fatal("request path wrote to database")
	}
	svc.EndSession("cam1")
	calls := repo.getServeCountCalls()
	if len(calls) != 1 || calls[0].Counts != [4]int64{100, 0, 0, 0} {
		t.Fatalf("expected one aggregated update: %+v", calls)
	}
}

func TestCloseWaitsForServingRequest(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	repo := &blockingServeRepo{
		fakeRepo: fakeRepo{segments: []models.LiveHLSSegment{{StreamID: "cam1", SegmentName: "missing.ts"}}},
		entered:  entered, release: release,
	}
	svc := &Service{Repository: repo}
	served := make(chan struct{})
	go func() {
		svc.ServeMedia(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), "cam1", "missing.ts")
		close(served)
	}()
	<-entered
	closed := make(chan struct{})
	go func() { svc.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close returned before request finished")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	<-served
	<-closed
	if got := repo.getSegment("cam1", "missing.ts").MissCount; got != 1 {
		t.Fatalf("missing in-flight outcome: %d", got)
	}
}

func TestServeMediaAfterClose(t *testing.T) {
	repo := &fakeRepo{segments: []models.LiveHLSSegment{{StreamID: "cam1", SegmentName: "missing.ts"}}}
	svc := &Service{Repository: repo}
	svc.Close()
	if svc.ServeMedia(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), "cam1", "missing.ts") {
		t.Fatal("closed service accepted request")
	}
	if len(repo.getServeCountCalls()) != 0 {
		t.Fatal("closed service wrote counts")
	}
}

func TestZeroValueServiceStartsPeriodicFlush(t *testing.T) {
	repo := &fakeRepo{segments: []models.LiveHLSSegment{{StreamID: "cam1", SegmentName: "missing.ts"}}}
	svc := &Service{Repository: repo}
	svc.ServeMedia(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), "cam1", "missing.ts")
	svc.mu.Lock()
	started := svc.stopFlush != nil
	svc.mu.Unlock()
	if !started {
		t.Fatal("zero-value service did not start flush ticker")
	}
	svc.Close()
}

func TestCloseBoundsFailedBatchAcrossKeys(t *testing.T) {
	repo := &deadlineServeRepo{}
	svc := &Service{Repository: repo}
	segment := &models.LiveHLSSegment{}
	for i := 0; i < 10; i++ {
		svc.incServeCount(segment, "cam1", fmt.Sprintf("segment-%d.ts", i), "miss")
	}
	start := time.Now()
	svc.Close()
	if elapsed := time.Since(start); elapsed > 4*time.Second {
		t.Fatalf("close waited per key: %v", elapsed)
	}
	svc.mu.Lock()
	remaining := len(svc.counts)
	svc.mu.Unlock()
	if remaining != 10 {
		t.Fatalf("lost failed deltas: %d keys", remaining)
	}
}

type deadlineServeRepo struct{ fakeRepo }

func (*deadlineServeRepo) AddServeCounts(ctx context.Context, _ string, _ string, _, _, _, _ int64) error {
	<-ctx.Done()
	return ctx.Err()
}

type blockingServeRepo struct {
	fakeRepo
	entered, release chan struct{}
}

func (r *blockingServeRepo) GetByStreamAndSegment(streamID, segmentName string) (*models.LiveHLSSegment, error) {
	close(r.entered)
	<-r.release
	return r.fakeRepo.GetByStreamAndSegment(streamID, segmentName)
}

func TestServeMediaPresignEmptyOrFailedFallbackToProxySuccess(t *testing.T) {
	for _, tc := range []struct {
		name    string
		link    string
		linkErr error
	}{
		{name: "presign_failed_err", link: "", linkErr: fmt.Errorf("presign failed")},
		{name: "presign_empty_link", link: "", linkErr: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &fakeRepo{segments: []models.LiveHLSSegment{
				{
					StreamID:    "cam1",
					SegmentName: "seg10.ts",
					StorageKey:  "live-hls/cam1/seg10.ts",
				},
			}}
			svc := &Service{
				Repository: repo,
				provider: &fallbackMockProvider{
					link:          tc.link,
					linkErr:       tc.linkErr,
					body:          "proxied-content",
					contentType:   "video/mp2t",
					contentLength: int64(len("proxied-content")),
				},
			}
			req := httptest.NewRequest(http.MethodGet, "/media/cam1/seg10.ts", nil)
			w := httptest.NewRecorder()

			if !svc.ServeMedia(w, req, "cam1", "seg10.ts") {
				t.Fatalf("expected ServeMedia to succeed via proxy fallback")
			}
			res := w.Result()
			defer res.Body.Close()
			if res.StatusCode != http.StatusOK {
				t.Fatalf("expected status 200, got %d", res.StatusCode)
			}
			body, _ := io.ReadAll(res.Body)
			if string(body) != "proxied-content" {
				t.Fatalf("unexpected body %q", string(body))
			}
			svc.EndSession("cam1")

			calls := repo.waitForServeCountCalls(1, time.Second)
			if len(calls) != 1 {
				t.Fatalf("expected 1 ServeCountCall, got %d", len(calls))
			}
			if call := calls[0]; call.StreamID != "cam1" || call.SegmentName != "seg10.ts" || call.Outcome != "proxy" {
				t.Fatalf("unexpected ServeCountCall: %+v", call)
			}
			seg := repo.getSegment("cam1", "seg10.ts")
			if seg == nil || seg.ProxyCount != 1 {
				t.Fatalf("expected ProxyCount 1, got %v", seg)
			}
		})
	}
}

func TestServeMediaPresignFailureAndProxyFailureIncrementsMiss(t *testing.T) {
	repo := &fakeRepo{segments: []models.LiveHLSSegment{
		{
			StreamID:    "cam1",
			SegmentName: "seg10.ts",
			StorageKey:  "live-hls/cam1/seg10.ts",
		},
	}}
	svc := &Service{
		Repository: repo,
		provider: &fallbackMockProvider{
			link:    "",
			linkErr: fmt.Errorf("link generation error"),
			readErr: fmt.Errorf("proxy read error"),
		},
	}
	req := httptest.NewRequest(http.MethodGet, "/media/cam1/seg10.ts", nil)
	w := httptest.NewRecorder()

	if svc.ServeMedia(w, req, "cam1", "seg10.ts") {
		t.Fatalf("expected ServeMedia to return false when both presign and proxy fail")
	}
	svc.EndSession("cam1")

	calls := repo.waitForServeCountCalls(1, time.Second)
	if len(calls) != 1 {
		t.Fatalf("expected 1 ServeCountCall for miss, got %d", len(calls))
	}
	if call := calls[0]; call.StreamID != "cam1" || call.SegmentName != "seg10.ts" || call.Outcome != "miss" {
		t.Fatalf("unexpected ServeCountCall: %+v", call)
	}
	seg := repo.getSegment("cam1", "seg10.ts")
	if seg == nil || seg.MissCount != 1 {
		t.Fatalf("expected MissCount 1, got %v", seg)
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

func TestRenderPlaylistOmitProgramDateTimeWhenSeekingDisabled(t *testing.T) {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	initSeg := "stream1_init.mp4"
	var segs []models.LiveHLSSegment
	for i := 0; i < 10; i++ {
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

	// 1. Sliding window (SegmentCount > 0 => seeking disabled) must NOT contain PROGRAM-DATE-TIME
	svcSliding := &Service{
		Repository:   repo,
		SegmentCount: 5,
	}
	plSliding, ok, err := svcSliding.RenderPlaylist("stream1/video/720", now.Add(20*time.Second))
	if err != nil || !ok {
		t.Fatalf("unexpected error rendering sliding playlist: %v", err)
	}
	if strings.Contains(string(plSliding), "#EXT-X-PROGRAM-DATE-TIME") {
		t.Fatalf("expected sliding window (seeking disabled) to omit #EXT-X-PROGRAM-DATE-TIME, got:\n%s", plSliding)
	}

	// 2. Full DVR (SegmentCount = 0 => seeking enabled) MUST contain PROGRAM-DATE-TIME
	svcFullDVR := &Service{
		Repository:   repo,
		SegmentCount: 0,
	}
	plFullDVR, ok, err := svcFullDVR.RenderPlaylist("stream1/video/720", now.Add(20*time.Second))
	if err != nil || !ok {
		t.Fatalf("unexpected error rendering full DVR playlist: %v", err)
	}
	if !strings.Contains(string(plFullDVR), "#EXT-X-PROGRAM-DATE-TIME") {
		t.Fatalf("expected full DVR (seeking enabled) to contain #EXT-X-PROGRAM-DATE-TIME, got:\n%s", plFullDVR)
	}
}

func TestServeMediaRedirectsWithCDNStorageProvider(t *testing.T) {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	fileID := "cdn-file-record-uuid-001"
	offset := int64(100)
	size := int64(1024)
	etag := fmt.Sprintf("%s:%d:%d", fileID, offset, size)

	repo := &fakeRepo{segments: []models.LiveHLSSegment{
		{
			StreamID:    "stream-cdn",
			SegmentName: "seg001.mp4",
			StorageKey:  "live-hls/stream-cdn/seg001.mp4",
			StorageETag: etag,
			StartedAt:   now,
		},
	}}

	expireAt := time.Now().Add(2 * time.Hour).UnixNano()
	rawSig := []byte("cdn-token-signature")
	stdSig := base64.StdEncoding.EncodeToString(rawSig)
	urlSig := base64.RawURLEncoding.EncodeToString(rawSig)

	cdnServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/getFileRecord/") {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"ID":     fileID,
				"status": 2,
			})
			return
		}
		if r.URL.Path == "/getTicket" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"version":        1,
				"file_record_id": fileID,
				"signature":      stdSig,
				"expire_at_ns":   expireAt,
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer cdnServer.Close()

	hubURL := "https://hub.aioz.network"
	cdnProv, err := hlss3uploader.NewCDNStorageProvider(cdnServer.URL, hubURL, "")
	if err != nil {
		t.Fatalf("unexpected err creating cdn provider: %v", err)
	}

	svc := &Service{
		Repository: repo,
		provider:   cdnProv,
	}

	req := httptest.NewRequest(http.MethodGet, "/media/stream-cdn/seg001.mp4", nil)
	w := httptest.NewRecorder()

	if !svc.ServeMedia(w, req, "stream-cdn", "seg001.mp4") {
		t.Fatalf("expected ServeMedia to return true for redirect")
	}

	res := w.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusFound {
		t.Fatalf("unexpected status %d, expected 302 StatusFound", res.StatusCode)
	}

	expectedLink := fmt.Sprintf("%s/file/%s?expire=%d&signature=%s&range=%d,%d",
		hubURL, fileID, expireAt, urlSig, offset, size)
	if got := res.Header.Get("Location"); got != expectedLink {
		t.Fatalf("unexpected Location header %q, expected %q", got, expectedLink)
	}
}
