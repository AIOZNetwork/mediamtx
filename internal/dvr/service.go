package dvr

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/bluenviron/mediamtx/internal/hlss3uploader"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/models"
)

const Window = 2 * time.Hour

type Service struct {
	Config     hlss3uploader.StorageConfig
	Repository models.LiveHLSSegmentRepository
	Parent     logger.Writer

	provider hlss3uploader.StorageProvider
	mu       sync.Mutex
	sessions map[string]string
	active   map[string]bool
	seqs     map[string]int64
}

func (s *Service) Initialize() {
	s.sessions = make(map[string]string)
	s.active = make(map[string]bool)
	s.seqs = make(map[string]int64)
	if s.Config.Prefix == "" {
		s.Config.Prefix = "live-hls"
	}
	provider, err := hlss3uploader.NewProviderSelector().Select(context.Background(), s.Config)
	if err != nil {
		s.log(logger.Info, "DVR external storage disabled: %v", err)
		return
	}
	s.provider = provider
	s.log(logger.Info, "DVR storage initialized (provider: %s, bucket: %s, endpoint: %s)", provider.Name(), s.Config.Bucket, s.Config.Endpoint)
}

func (s *Service) Close() {
	if s.provider != nil {
		_ = s.provider.Close()
	}
}

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

func isRecordableSegment(localPath string) bool {
	ext := strings.ToLower(filepath.Ext(localPath))
	return ext == ".ts" || ext == ".mp4" || ext == ".m4s"
}

func contentTypeForExt(localPath string) string {
	switch strings.ToLower(filepath.Ext(localPath)) {
	case ".mp4":
		return "video/mp4"
	case ".m4s":
		return "video/iso.segment"
	case ".ts":
		return "video/mp2t"
	default:
		return "application/octet-stream"
	}
}

func (s *Service) RecordSegment(streamID, localPath string, duration time.Duration) {
	if s == nil || s.Repository == nil || !isRecordableSegment(localPath) {
		return
	}
	info, err := os.Stat(localPath)
	if err != nil || info.Size() == 0 {
		return
	}

	sequence := s.nextSequence(streamID)
	startedAt := info.ModTime().Add(-duration)
	if duration <= 0 {
		startedAt = info.ModTime()
	}
	storageKey := path.Join(strings.Trim(s.Config.Prefix, "/"), "dvr", streamID, filepath.Base(localPath))

	segment := models.LiveHLSSegment{
		StreamID:       streamID,
		SessionID:      s.sessionID(streamID),
		SegmentName:    filepath.Base(localPath),
		LocalPath:      localPath,
		StorageBackend: "local",
		StorageKey:     storageKey,
		StartedAt:      startedAt,
		DurationMS:     duration.Milliseconds(),
		SizeBytes:      info.Size(),
		Sequence:       &sequence,
		ContentType:    contentTypeForExt(localPath),
	}

	_ = s.Repository.UpsertLocal(segment)
	if s.provider == nil {
		return
	}

	segment.StorageBackend = s.provider.Name()
	_ = s.Repository.UpsertUploading(segment)
	uploadCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	etag, err := s.provider.UploadFile(uploadCtx, localPath, storageKey, segment.ContentType)
	cancel()
	if err != nil {
		_ = s.Repository.UpsertUploadFailed(segment)
		s.log(logger.Warn, "failed to upload DVR segment %s: %v", localPath, err)
		return
	}
	now := time.Now()
	segment.StorageETag = etag
	segment.UploadedAt = &now
	_ = s.Repository.UpsertUploaded(segment)
}

func isFMP4Segment(name string) bool {
	ext := strings.ToLower(filepath.Ext(name))
	return ext == ".mp4" || ext == ".m4s" || ext == ".mp"
}

func isInitSegment(name string) bool {
	lower := strings.ToLower(name)
	return strings.Contains(lower, "init")
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
		if !seg.StartedAt.IsZero() {
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
		if !seg.StartedAt.IsZero() {
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

	// 2. Fallback to S3 presigned URL
	storageKey := ""
	if segment != nil && segment.StorageKey != "" {
		storageKey = segment.StorageKey
	} else {
		prefix := strings.Trim(s.Config.Prefix, "/")
		if prefix == "" {
			prefix = "live-hls"
		}
		storageKey = path.Join(prefix, streamID, segmentName)
	}

	if storageKey != "" && s.provider != nil {
		if signedURL, err := s.presign(storageKey); err == nil && signedURL != "" {
			http.Redirect(w, r, signedURL, http.StatusFound)
			return true
		}
	}

	return false
}

func (s *Service) nextSequence(streamID string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seqs == nil {
		s.seqs = make(map[string]int64)
	}
	if seq, ok := s.seqs[streamID]; ok {
		seq++
		s.seqs[streamID] = seq
		return seq
	}
	if max, ok, err := s.Repository.MaxSequence(streamID); err == nil && ok {
		s.seqs[streamID] = max + 1
		return max + 1
	}
	seq := time.Now().UnixNano()
	s.seqs[streamID] = seq
	return seq
}

func (s *Service) sessionID(streamID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessions == nil {
		s.sessions = make(map[string]string)
	}
	if id := s.sessions[streamID]; id != "" {
		return id
	}
	id := fmt.Sprintf("%s-%d", strings.ReplaceAll(streamID, "/", "_"), time.Now().UnixNano())
	s.sessions[streamID] = id
	return id
}

func (s *Service) currentSessionID(streamID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[streamID]
}

func (s *Service) presign(remoteKey string) (string, error) {
	s3Provider, ok := s.provider.(*hlss3uploader.S3StorageProvider)
	if !ok {
		return "", fmt.Errorf("provider does not support presign")
	}
	req, err := s3Provider.PresignClient().PresignGetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(s3Provider.Bucket()),
		Key:    aws.String(remoteKey),
	}, s3.WithPresignExpires(15*time.Minute))
	if err != nil {
		return "", err
	}
	return req.URL, nil
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
