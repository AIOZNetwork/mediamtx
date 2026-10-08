package dvr

import (
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

func (r *fakeRepo) UpsertLocal(models.LiveHLSSegment) error                { return nil }
func (r *fakeRepo) UpsertUploading(models.LiveHLSSegment) error            { return nil }
func (r *fakeRepo) UpsertUploaded(models.LiveHLSSegment) error             { return nil }
func (r *fakeRepo) UpsertUploadFailed(models.LiveHLSSegment) error         { return nil }
func (r *fakeRepo) MarkLocalDeleted(string, string, time.Time) error       { return nil }
func (r *fakeRepo) GetByStorageKey(string) (*models.LiveHLSSegment, error) { return nil, nil }
func (r *fakeRepo) GetByStreamAndSegment(streamID, segmentName string) (*models.LiveHLSSegment, error) {
	for _, segment := range r.segments {
		if segment.StreamID == streamID && segment.SegmentName == segmentName {
			return &segment, nil
		}
	}
	return nil, nil
}
func (r *fakeRepo) MaxSequence(string) (int64, bool, error) { return 0, false, nil }
func (r *fakeRepo) ListWindow(streamID, sessionID string, since time.Time) ([]models.LiveHLSSegment, error) {
	var out []models.LiveHLSSegment
	for _, segment := range r.segments {
		if segment.StreamID == streamID && (sessionID == "" || segment.SessionID == sessionID) && !segment.StartedAt.Before(since) {
			out = append(out, segment)
		}
	}
	return out, nil
}
func (r *fakeRepo) ListFMP4Window(streamID string, since time.Time) ([]models.LiveHLSSegment, error) {
	var out []models.LiveHLSSegment
	for _, segment := range r.segments {
		if segment.StreamID == streamID && !segment.StartedAt.Before(since) {
			out = append(out, segment)
		}
	}
	return out, nil
}

func TestRenderFMP4PlaylistUsesLocalMetadata(t *testing.T) {
	now := time.Now().UTC()
	seq := int64(7)
	svc := &Service{Repository: &fakeRepo{segments: []models.LiveHLSSegment{
		{StreamID: "cam", SegmentName: "init.mp4", StartedAt: now},
		{StreamID: "cam", SegmentName: "seg7.m4s", InitSegmentName: "init.mp4", MediaSequence: &seq, DurationMS: 1500, StartedAt: now},
	}}}

	playlist, ok, err := svc.RenderPlaylist("cam", now)
	if err != nil || !ok {
		t.Fatalf("RenderPlaylist() ok=%v err=%v", ok, err)
	}
	want := "#EXT-X-MAP:URI=\"/media/cam/init.mp4\""
	if string(playlist) == "" || !strings.Contains(string(playlist), want) {
		t.Fatalf("playlist missing %q:\n%s", want, playlist)
	}
	if !strings.Contains(string(playlist), "#EXT-X-ENDLIST\n") {
		t.Fatalf("inactive fMP4 playlist is missing ENDLIST:\n%s", playlist)
	}
}

func TestServeMediaServesLocalObjectOnly(t *testing.T) {
	file := filepath.Join(t.TempDir(), "segment.m4s")
	if err := os.WriteFile(file, []byte("segment"), 0o600); err != nil {
		t.Fatal(err)
	}
	svc := &Service{Repository: &fakeRepo{segments: []models.LiveHLSSegment{{
		StreamID: "cam", SegmentName: "segment.m4s", LocalPath: file,
	}}}}
	w := httptest.NewRecorder()
	if !svc.ServeMedia(w, httptest.NewRequest(http.MethodGet, "/media/cam/segment.m4s", nil), "cam", "segment.m4s") {
		t.Fatal("expected local media")
	}
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d", w.Code)
	}
	if svc.ServeMedia(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/media/cam/missing.m4s", nil), "cam", "missing.m4s") {
		t.Fatal("remote provider fallback must be disabled")
	}
}
