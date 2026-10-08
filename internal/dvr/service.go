package dvr

import (
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/models"
)

const (
	Window               = 2 * time.Hour
	mediaTypeInitKeyword = "init"

	contentTypeMP4     = "video/mp4"
	contentTypeM4S     = "video/iso.segment"
	contentTypeTS      = "video/mp2t"
	contentTypeDefault = "application/octet-stream"
)

type Service struct {
	Repository   models.LiveHLSSegmentRepository
	Parent       logger.Writer
	SegmentCount int

	mu       sync.Mutex
	sessions map[string]string
	active   map[string]bool
}

func (s *Service) Initialize() {
	s.sessions = make(map[string]string)
	s.active = make(map[string]bool)
}

func (s *Service) Close() {}

func (s *Service) StartSession(streamID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessions == nil {
		s.sessions = make(map[string]string)
	}
	s.sessions[streamID] = fmt.Sprintf("%s-%d", strings.ReplaceAll(streamID, "/", "_"), time.Now().UnixNano())
	if s.active == nil {
		s.active = make(map[string]bool)
	}
	s.active[streamID] = true
}

func (s *Service) EndSession(streamID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active == nil {
		s.active = make(map[string]bool)
	}
	s.active[streamID] = false
}

func (s *Service) IsActive(streamID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active[streamID]
}

func contentTypeForExt(localPath string) string {
	switch strings.ToLower(filepath.Ext(localPath)) {
	case ".mp4":
		return contentTypeMP4
	case ".m4s":
		return contentTypeM4S
	case ".ts":
		return contentTypeTS
	default:
		return contentTypeDefault
	}
}

func isFMP4Segment(name string) bool {
	ext := strings.ToLower(filepath.Ext(name))
	return ext == ".mp4" || ext == ".m4s" || ext == ".mp"
}

func isInitSegment(name string) bool {
	lower := strings.ToLower(name)
	return strings.Contains(lower, mediaTypeInitKeyword)
}

func (s *Service) RenderPlaylist(streamID string, now time.Time) ([]byte, bool, error) {
	if s == nil || s.Repository == nil {
		return nil, false, nil
	}
	if playlist, ok, err := s.renderFMP4Playlist(streamID, now); err != nil || ok {
		return playlist, ok, err
	}
	segments, err := s.Repository.ListWindow(streamID, s.currentSessionID(streamID), now.Add(-Window))
	if err != nil || len(segments) == 0 {
		return nil, false, err
	}
	sort.SliceStable(segments, func(i, j int) bool {
		if segments[i].Sequence == nil || segments[j].Sequence == nil {
			return segments[i].StartedAt.Before(segments[j].StartedAt)
		}
		return *segments[i].Sequence < *segments[j].Sequence
	})

	var mediaSegments []models.LiveHLSSegment

	for _, seg := range segments {
		if !isInitSegment(seg.SegmentName) {
			mediaSegments = append(mediaSegments, seg)
		}
	}

	if len(mediaSegments) == 0 {
		return nil, false, nil
	}

	if s.SegmentCount > 0 && len(mediaSegments) > s.SegmentCount {
		mediaSegments = mediaSegments[len(mediaSegments)-s.SegmentCount:]
	}

	mediaSeq := int64(0)
	if mediaSegments[0].Sequence != nil {
		mediaSeq = *mediaSegments[0].Sequence
	}
	target := 1
	for _, seg := range mediaSegments {
		if v := int(math.Ceil(float64(seg.DurationMS) / 1000)); v > target {
			target = v
		}
	}

	var b strings.Builder
	b.WriteString("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-INDEPENDENT-SEGMENTS\n")
	b.WriteString(fmt.Sprintf("#EXT-X-TARGETDURATION:%d\n", target))
	b.WriteString(fmt.Sprintf("#EXT-X-MEDIA-SEQUENCE:%d\n", mediaSeq))

	for _, seg := range mediaSegments {
		if s.SegmentCount == 0 && !seg.StartedAt.IsZero() {
			b.WriteString("#EXT-X-PROGRAM-DATE-TIME:")
			b.WriteString(seg.StartedAt.UTC().Format(time.RFC3339Nano))
			b.WriteByte('\n')
		}
		dur := float64(seg.DurationMS) / 1000
		if dur <= 0 {
			dur = 2.0
		}
		b.WriteString(fmt.Sprintf("#EXTINF:%.3f,\n", dur))
		b.WriteString("/media/")
		b.WriteString(escapePath(streamID))
		b.WriteByte('/')
		b.WriteString(url.PathEscape(seg.SegmentName))
		b.WriteByte('\n')
	}
	if !s.IsActive(streamID) {
		b.WriteString("#EXT-X-ENDLIST\n")
	}
	return []byte(b.String()), true, nil
}

func (s *Service) renderFMP4Playlist(streamID string, now time.Time) ([]byte, bool, error) {
	segments, err := s.Repository.ListFMP4Window(streamID, now.Add(-Window))
	if err != nil || len(segments) == 0 {
		return nil, false, err
	}
	var mediaSegments []models.LiveHLSSegment
	for _, seg := range segments {
		if isFMP4Segment(seg.SegmentName) && !isInitSegment(seg.SegmentName) && seg.InitSegmentName != "" {
			mediaSegments = append(mediaSegments, seg)
		}
	}
	if len(mediaSegments) == 0 {
		return nil, false, nil
	}

	if s.SegmentCount > 0 && len(mediaSegments) > s.SegmentCount {
		mediaSegments = mediaSegments[len(mediaSegments)-s.SegmentCount:]
	}

	mediaSeq := int64(0)
	if mediaSegments[0].MediaSequence != nil {
		mediaSeq = *mediaSegments[0].MediaSequence
	} else if mediaSegments[0].Sequence != nil {
		mediaSeq = *mediaSegments[0].Sequence
	}
	target := 1
	for _, seg := range mediaSegments {
		if v := int(math.Ceil(float64(seg.DurationMS) / 1000)); v > target {
			target = v
		}
	}

	var b strings.Builder
	b.WriteString("#EXTM3U\n#EXT-X-VERSION:10\n#EXT-X-INDEPENDENT-SEGMENTS\n")
	b.WriteString(fmt.Sprintf("#EXT-X-TARGETDURATION:%d\n", target))
	b.WriteString(fmt.Sprintf("#EXT-X-MEDIA-SEQUENCE:%d\n", mediaSeq))

	lastInit := ""
	lastMuxSession := ""
	for i, seg := range mediaSegments {
		initChanged := seg.InitSegmentName != "" && seg.InitSegmentName != lastInit
		muxChanged := i > 0 && seg.MuxSessionID != "" && seg.MuxSessionID != lastMuxSession
		if i > 0 && (initChanged || muxChanged) {
			b.WriteString("#EXT-X-DISCONTINUITY\n")
		}
		if initChanged {
			b.WriteString(fmt.Sprintf("#EXT-X-MAP:URI=\"/media/%s/%s\"\n", escapePath(streamID), url.PathEscape(seg.InitSegmentName)))
			lastInit = seg.InitSegmentName
		}
		if seg.MuxSessionID != "" {
			lastMuxSession = seg.MuxSessionID
		}
		if s.SegmentCount == 0 && !seg.StartedAt.IsZero() {
			b.WriteString("#EXT-X-PROGRAM-DATE-TIME:")
			b.WriteString(seg.StartedAt.UTC().Format(time.RFC3339Nano))
			b.WriteByte('\n')
		}
		dur := float64(seg.DurationMS) / 1000
		if dur <= 0 {
			dur = 2.0
		}
		b.WriteString(fmt.Sprintf("#EXTINF:%.3f,\n", dur))
		b.WriteString("/media/")
		b.WriteString(escapePath(streamID))
		b.WriteByte('/')
		b.WriteString(url.PathEscape(seg.SegmentName))
		b.WriteByte('\n')
	}
	if !s.IsActive(streamID) {
		b.WriteString("#EXT-X-ENDLIST\n")
	}
	return []byte(b.String()), true, nil
}

func (s *Service) ServeMedia(w http.ResponseWriter, r *http.Request, streamID, segmentName string) bool {
	if s == nil {
		return false
	}

	var segment *models.LiveHLSSegment
	if s.Repository != nil {
		seg, err := s.Repository.GetByStreamAndSegment(streamID, segmentName)
		if err == nil && seg != nil {
			segment = seg
		}
	}

	// 1. Try local file if available on disk
	localPath := ""
	if segment != nil && segment.LocalPath != "" {
		localPath = segment.LocalPath
	} else {
		localPath = filepath.Join("./input-live", streamID, segmentName)
	}

	if localPath != "" {
		if _, err := os.Stat(localPath); err == nil {
			ct := ""
			if segment != nil && segment.ContentType != "" {
				ct = segment.ContentType
			} else {
				ct = contentTypeForExt(localPath)
			}
			w.Header().Set("Content-Type", ct)
			http.ServeFile(w, r, localPath)
			return true
		}
	}

	return false
}

func (s *Service) currentSessionID(streamID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[streamID]
}

func (s *Service) log(level logger.Level, format string, args ...interface{}) {
	if s.Parent != nil {
		s.Parent.Log(level, "[DVR] "+format, args...)
	}
}

func escapePath(v string) string {
	parts := strings.Split(v, "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	return strings.Join(parts, "/")
}
