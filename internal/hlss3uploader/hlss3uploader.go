package hlss3uploader

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fsnotify/fsnotify"

	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/models"
)

// StorageProvider is the interface for pluggable storage backends (S3, Local, CDN, MinIO, GCS, etc.).
type StorageProvider interface {
	Name() string
	UploadFile(ctx context.Context, localPath, remoteKey, contentType string) (string, error)
	DeleteFolder(ctx context.Context, prefix string) error
	Close() error
}

type ReadableStorageProvider interface {
	GetObject(ctx context.Context, key string) (io.ReadCloser, string, int64, error)
}

// HLSS3Uploader watches local HLS directory (e.g. ./input-live),
// uploads generated segment and playlist files to configured StorageProvider,
// and deletes local segment files after successful upload.
type HLSS3Uploader struct {
	Config     StorageConfig
	Parent     logger.Writer
	Repository models.LiveHLSSegmentRepository

	provider StorageProvider

	ctx       context.Context
	ctxCancel func()
	watcher   *fsnotify.Watcher

	taskChan chan string
	done     chan struct{}
	wg       sync.WaitGroup

	processingFiles sync.Map
	uploadedFiles   sync.Map
}

const (
	defaultStoragePrefix         = "live-hls"
	defaultStorageDirectory      = "./input-live"
	defaultWorkerCount           = 4
	defaultTaskQueueSize         = 1000
	defaultSegmentDurationMS     = 2000
	defaultUploadTimeout         = 2 * time.Minute
	defaultPresignExpires        = 15 * time.Minute
	cacheControlNoCache          = "no-cache"
	cacheControlImmutableSegment = "public, max-age=31536000, immutable"

	contentTypeMP4     = "video/mp4"
	contentTypeM4S     = "video/iso.segment"
	contentTypeTS      = "video/mp2t"
	contentTypeM3U8    = "application/x-mpegURL"
	contentTypeDefault = "application/octet-stream"

	extM3U8 = ".m3u8"
	extMP4  = ".mp4"
	extMP   = ".mp"
	extM4S  = ".m4s"
	extTS   = ".ts"
)

// Initialize initializes and starts the HLSS3Uploader background service using supplied Config.
func (u *HLSS3Uploader) Initialize() error {
	u.ctx, u.ctxCancel = context.WithCancel(context.Background())
	u.done = make(chan struct{})
	u.taskChan = make(chan string, defaultTaskQueueSize)
	if u.Config.Workers <= 0 {
		u.Config.Workers = defaultWorkerCount
	}

	selector := NewProviderSelector()
	provider, err := selector.Select(u.ctx, u.Config)
	if err != nil {
		u.Log(logger.Info, "HLS Storage Uploader disabled: %v", err)
		close(u.done)
		return nil
	}
	u.provider = provider

	if u.Config.Directory == "" {
		u.Config.Directory = defaultStorageDirectory
	}
	if u.Config.Prefix == "" {
		u.Config.Prefix = defaultStoragePrefix
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		u.Log(logger.Error, "failed to create fsnotify watcher: %v", err)
		close(u.done)
		return err
	}
	u.watcher = watcher

	u.Log(logger.Info, "HLS Storage Uploader initialized (provider: %s, bucket: %s, endpoint: %s, dir: %s)", u.provider.Name(), u.Config.Bucket, u.Config.Endpoint, u.Config.Directory)

	for i := 0; i < u.Config.Workers; i++ {
		u.wg.Add(1)
		go u.workerLoop()
	}

	// Start directory watcher loop
	go u.watchLoop()

	return nil
}

// Close stops the HLSS3Uploader and waits for goroutines to exit.
func (u *HLSS3Uploader) Close() {
	if u.ctxCancel != nil {
		u.ctxCancel()
	}
	if u.watcher != nil {
		u.watcher.Close()
	}
	if u.done != nil {
		// Wait for watchLoop to exit, with a timeout
		select {
		case <-u.done:
		case <-time.After(5 * time.Second):
			u.Parent.Log(logger.Warn, "[HLS Uploader Close] watchLoop did not exit in time")
		}
	}

	// Wait for workers to finish, with a timeout
	waitDone := make(chan struct{})
	go func() {
		u.wg.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		u.Parent.Log(logger.Warn, "[HLS Uploader Close] workers did not exit in time")
	}

	// Note: We intentionally do NOT delete S3 segments on Close().
	// Previous versions called provider.DeleteFolder() here, which destroyed
	// DVR/playback history whenever a muxer restarted, hit idle timeout, or
	// errored. S3 lifecycle policies should handle cleanup instead.
	if u.provider != nil {
		u.provider.Close()
	}
}

// Log implements logger.Writer.
func (u *HLSS3Uploader) Log(level logger.Level, format string, args ...interface{}) {
	if u.Parent != nil {
		u.Parent.Log(level, "[HLS Storage Uploader] "+format, args...)
	}
}

// EnqueueSegment is called by the Muxer callback when a segment file is finalized.
// It queues the file for immediate upload to S3 without polling.
func (u *HLSS3Uploader) EnqueueSegment(filePath string, duration time.Duration) {
	u.sendToTaskChan(filePath)
}

func (u *HLSS3Uploader) watchLoop() {
	defer close(u.done)

	// Ensure directory exists
	os.MkdirAll(u.Config.Directory, 0755)

	// Add root directory to fsnotify for directory-create and playlist-write events.
	if err := u.watcher.Add(u.Config.Directory); err != nil {
		u.Log(logger.Warn, "failed to watch directory %s: %v", u.Config.Directory, err)
	}

	// Add existing subdirectories and queue existing files
	u.scanDirectory(u.Config.Directory)

	for {
		select {
		case <-u.ctx.Done():
			return

		case event, ok := <-u.watcher.Events:
			if !ok {
				return
			}

			ext := strings.ToLower(filepath.Ext(event.Name))

			isSegment := ext == ".m4s" || ext == ".ts" || ext == ".mp4" || ext == ".mp"

			// New sub-directory: add to fsnotify
			if event.Op&fsnotify.Create != 0 {
				info, err := os.Stat(event.Name)
				if err == nil && info.IsDir() {
					u.watcher.Add(event.Name)
					u.scanDirectory(event.Name)
					continue
				}
			}

			// Playlist file: upload to S3 on every Write/Create
			// Segment file: enqueue for waitStable + upload
			if (ext == ".m3u8" || isSegment) && event.Op&(fsnotify.Create|fsnotify.Write) != 0 {
				u.sendToTaskChan(event.Name)
			}

			if isSegment && event.Op&fsnotify.Remove != 0 {
				go u.handleLocalRemove(event.Name)
			}

		case err, ok := <-u.watcher.Errors:
			if !ok {
				return
			}
			u.Log(logger.Warn, "watcher error: %v", err)
		}
	}
}

func (u *HLSS3Uploader) scanDirectory(dir string) {
	filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			u.watcher.Add(path)
		} else {
			ext := strings.ToLower(filepath.Ext(path))
			isSegment := ext == ".m4s" || ext == ".ts" || ext == ".mp4" || ext == ".mp"
			if ext == ".m3u8" || isSegment {
				u.sendToTaskChan(path)
			}
		}
		return nil
	})
}

// sendToTaskChan sends filePath to the upload worker queue without blocking.
func (u *HLSS3Uploader) sendToTaskChan(filePath string) {
	select {
	case u.taskChan <- filePath:
	default:
		// Queue full — retry in a background goroutine.
		go func(p string) {
			select {
			case u.taskChan <- p:
			case <-u.ctx.Done():
			}
		}(filePath)
	}
}

func (u *HLSS3Uploader) handleLocalRemove(filePath string) {
	if u.Repository == nil {
		return
	}

	relPath, err := filepath.Rel(u.Config.Directory, filePath)
	if err != nil {
		relPath = filepath.Base(filePath)
	}
	relPath = filepath.ToSlash(relPath)

	streamName := strings.Trim(strings.TrimSpace(u.Config.StreamName), "/")
	if streamName == "" {
		streamName = filepath.Base(u.Config.Directory)
	}

	segmentName := filepath.Base(filePath)
	_ = u.Repository.MarkLocalDeleted(streamName, segmentName, time.Now())
}

func (u *HLSS3Uploader) workerLoop() {
	defer u.wg.Done()

	for {
		select {
		case <-u.ctx.Done():
			return
		case filePath, ok := <-u.taskChan:
			if !ok {
				return
			}
			u.processFile(filePath)
		}
	}
}

func (u *HLSS3Uploader) processFile(filePath string) {
	relPath, err := filepath.Rel(u.Config.Directory, filePath)
	if err != nil {
		relPath = filepath.Base(filePath)
	}
	relPath = filepath.ToSlash(relPath)

	ext := strings.ToLower(filepath.Ext(filePath))
	isSegment := ext == ".m4s" || ext == ".ts" || ext == ".mp4" || ext == ".mp"

	streamName := strings.Trim(strings.TrimSpace(u.Config.StreamName), "/")
	keyRelPath := relPath
	if streamName != "" && !strings.HasPrefix(keyRelPath, streamName+"/") {
		keyRelPath = streamName + "/" + keyRelPath
	}
	remoteKey := fmt.Sprintf("%s/%s", strings.TrimSuffix(u.Config.Prefix, "/"), keyRelPath)

	// Segments: deduplicate — never re-upload a successfully completed segment.
	if isSegment {
		if strings.Contains(filepath.Base(filePath), "_part") {
			// Skip uploading LL-HLS parts. The full segment will be uploaded instead.
			return
		}
		if _, exists := u.uploadedFiles.Load(remoteKey); exists {
			return
		}
	}

	// Prevent concurrent in-flight uploads for the same remote key.
	if _, loaded := u.processingFiles.LoadOrStore(remoteKey, true); loaded {
		return
	}
	defer u.processingFiles.Delete(remoteKey)

	if !u.waitStable(filePath) {
		return
	}

	info, err := os.Stat(filePath)
	if err != nil || info.Size() == 0 {
		return
	}

	durationMS := u.inferDurationMS(filePath)
	if isInitSegmentFile(filepath.Base(filePath)) {
		durationMS = 0
	} else if durationMS <= 0 {
		durationMS = 2000 // default 2s segment duration per spec
	}
	duration := time.Duration(durationMS) * time.Millisecond
	startedAt := info.ModTime().Add(-duration)
	if duration <= 0 {
		startedAt = info.ModTime()
	}

	contentType := getContentType(ext)
	segmentRecord := models.LiveHLSSegment{
		StreamID:       streamName,
		SegmentName:    filepath.Base(filePath),
		LocalPath:      filePath,
		StorageBackend: u.provider.Name(),
		StorageKey:     remoteKey,
		StartedAt:      startedAt,
		DurationMS:     durationMS,
		SizeBytes:      info.Size(),
		ContentType:    contentType,
	}
	if segmentRecord.StreamID == "" {
		segmentRecord.StreamID = filepath.Base(u.Config.Directory)
	}
	populateABRMetadata(&segmentRecord, filepath.Base(filePath))
	if isSegment && u.Repository != nil {
		mergeExistingDVRMetadata(u.Repository, &segmentRecord)
	}
	if isSegment && u.Repository != nil {
		_ = u.Repository.UpsertLocal(segmentRecord)
		_ = u.Repository.UpsertUploading(segmentRecord)
	}

	uploadCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	etag, err := u.provider.UploadFile(uploadCtx, filePath, remoteKey, contentType)
	cancel()

	if err != nil {
		if isSegment && u.Repository != nil {
			_ = u.Repository.UpsertUploadFailed(segmentRecord)
		}
		u.Log(logger.Warn, "failed to upload %s via provider %s (key: %s, streamKey: %s): %v", relPath, u.provider.Name(), remoteKey, u.Config.StreamKey, err)
		return
	}

	if isSegment {
		now := time.Now()
		segmentRecord.StorageETag = etag
		segmentRecord.UploadedAt = &now
		if u.Repository != nil {
			_ = u.Repository.UpsertUploaded(segmentRecord)
		}
		u.uploadedFiles.Store(remoteKey, true)
	} else {
		if strings.HasSuffix(filepath.Base(filePath), "_stream.m3u8") && u.Repository != nil {
			u.ingestFMP4Playlist(filePath, streamName)
		}
		// u.Log(logger.Info, "uploaded playlist %s via %s (key: %s, streamKey: %s)", relPath, u.provider.Name(), remoteKey, u.Config.StreamKey)
	}
}

func getContentType(ext string) string {
	switch ext {
	case extM4S:
		return contentTypeM4S
	case extMP4, extMP:
		return contentTypeMP4
	case extTS:
		return contentTypeTS
	case extM3U8:
		return contentTypeM3U8
	default:
		return contentTypeDefault
	}
}

var (
	extinfRe        = regexp.MustCompile(`^#EXTINF:([0-9.]+)`)                               // #EXTINF:1.000,
	partRe          = regexp.MustCompile(`^#EXT-X-PART:.*DURATION=([0-9.]+).*URI="([^"]+)"`) // LL-HLS parts
	mapURIRe        = regexp.MustCompile(`^#EXT-X-MAP:.*URI="?([^",]+)"?`)                   // init segment
	mediaSequenceRe = regexp.MustCompile(`^#EXT-X-MEDIA-SEQUENCE:([0-9]+)`)                  // media sequence
	pdtRe           = regexp.MustCompile(`^#EXT-X-PROGRAM-DATE-TIME:(.+)$`)
	fmp4SegmentRe   = regexp.MustCompile(`_seg[0-9]+\.mp4$`)
)

func isInitSegmentFile(name string) bool {
	return strings.HasSuffix(strings.ToLower(name), "_init.mp4")
}

func (u *HLSS3Uploader) ingestFMP4Playlist(playlistPath string, streamName string) {
	entries, initName := parseFMP4Playlist(playlistPath)
	if len(entries) == 0 {
		return
	}
	playlistName := filepath.Base(playlistPath)
	childToken := strings.TrimSuffix(playlistName, "_stream.m3u8")
	if streamName == "" {
		streamName = filepath.Base(u.Config.Directory)
	}
	initStorageKey := ""
	if initName != "" {
		initStorageKey = u.remoteKeyForName(streamName, initName)
	}
	storageBackend := "local"
	if u.provider != nil {
		storageBackend = u.provider.Name()
	}
	for _, entry := range entries {
		segName := filepath.Base(entry.URI)
		localPath := filepath.Join(filepath.Dir(playlistPath), segName)
		size := int64(0)
		startedAt := entry.ProgramDateTime
		if startedAt.IsZero() {
			if existing, err := u.Repository.GetByStreamAndSegment(streamName, segName); err == nil && existing != nil {
				startedAt = existing.StartedAt
			}
		}
		if info, err := os.Stat(localPath); err == nil {
			size = info.Size()
			if startedAt.IsZero() && entry.DurationMS > 0 {
				startedAt = info.ModTime().Add(-time.Duration(entry.DurationMS) * time.Millisecond)
			}
		}
		sequence := entry.MediaSequence
		segment := models.LiveHLSSegment{
			StreamID:        streamName,
			SegmentName:     segName,
			LocalPath:       localPath,
			StorageBackend:  storageBackend,
			StorageKey:      u.remoteKeyForName(streamName, segName),
			StartedAt:       startedAt,
			DurationMS:      entry.DurationMS,
			SizeBytes:       size,
			ContentType:     getContentType(strings.ToLower(filepath.Ext(segName))),
			MediaSequence:   &sequence,
			InitSegmentName: initName,
			InitStorageKey:  initStorageKey,
			PlaylistName:    playlistName,
			MuxSessionID:    deriveMuxSessionID(segName, childToken),
		}
		populateABRMetadata(&segment, segName)
		if existing, err := u.Repository.GetByStreamAndSegment(streamName, segName); err == nil && existing != nil && existing.Status == models.LiveHLSSegmentStatusUploadedS3 {
			segment.StorageETag = existing.StorageETag
			segment.UploadedAt = existing.UploadedAt
			_ = u.Repository.UpsertUploaded(segment)
		} else {
			_ = u.Repository.UpsertLocal(segment)
		}
	}
}

type fmp4PlaylistEntry struct {
	URI             string
	DurationMS      int64
	MediaSequence   int64
	ProgramDateTime time.Time
}

func parseFMP4Playlist(playlistPath string) ([]fmp4PlaylistEntry, string) {
	byts, err := os.ReadFile(playlistPath)
	if err != nil {
		return nil, ""
	}
	var entries []fmp4PlaylistEntry
	var initName string
	mediaSequence := int64(0)
	pendingDuration := int64(0)
	var pendingPDT time.Time
	for _, line := range strings.Split(string(byts), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if matches := mediaSequenceRe.FindStringSubmatch(line); matches != nil {
			mediaSequence, _ = strconv.ParseInt(matches[1], 10, 64)
			continue
		}
		if matches := mapURIRe.FindStringSubmatch(line); matches != nil {
			initName = filepath.Base(matches[1])
			continue
		}
		if matches := pdtRe.FindStringSubmatch(line); matches != nil {
			pendingPDT, _ = time.Parse(time.RFC3339Nano, strings.TrimSpace(matches[1]))
			continue
		}
		if matches := extinfRe.FindStringSubmatch(line); matches != nil {
			pendingDuration = secondsToMS(matches[1])
			continue
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		entries = append(entries, fmp4PlaylistEntry{
			URI:             line,
			DurationMS:      pendingDuration,
			MediaSequence:   mediaSequence + int64(len(entries)),
			ProgramDateTime: pendingPDT,
		})
		pendingDuration = 0
		pendingPDT = time.Time{}
	}
	return entries, initName
}

func (u *HLSS3Uploader) remoteKeyForName(streamName, name string) string {
	keyRelPath := path.Join(strings.Trim(streamName, "/"), name)
	return path.Join(strings.TrimSuffix(u.Config.Prefix, "/"), keyRelPath)
}

func populateABRMetadata(segment *models.LiveHLSSegment, fileName string) {
	base, track, rendition := deriveABRPathMetadata(segment.StreamID)
	segment.BaseStreamID = base
	segment.TrackType = track
	segment.Rendition = rendition
	if segment.MuxSessionID == "" {
		childToken := strings.TrimSuffix(segment.PlaylistName, "_stream.m3u8")
		segment.MuxSessionID = deriveMuxSessionID(fileName, childToken)
	}
}

func mergeExistingDVRMetadata(repo models.LiveHLSSegmentRepository, segment *models.LiveHLSSegment) {
	if repo == nil || segment == nil {
		return
	}
	existing, err := repo.GetByStreamAndSegment(segment.StreamID, segment.SegmentName)
	if err != nil || existing == nil {
		return
	}
	if segment.BaseStreamID == "" {
		segment.BaseStreamID = existing.BaseStreamID
	}
	if segment.TrackType == "" || segment.TrackType == "unknown" {
		segment.TrackType = existing.TrackType
	}
	if segment.Rendition == "" {
		segment.Rendition = existing.Rendition
	}
	if segment.MuxSessionID == "" || strings.Contains(segment.MuxSessionID, "_seg") {
		segment.MuxSessionID = existing.MuxSessionID
	}
	if segment.InitSegmentName == "" {
		segment.InitSegmentName = existing.InitSegmentName
	}
	if segment.InitStorageKey == "" {
		segment.InitStorageKey = existing.InitStorageKey
	}
	if segment.PlaylistName == "" {
		segment.PlaylistName = existing.PlaylistName
	}
	if segment.MediaSequence == nil {
		segment.MediaSequence = existing.MediaSequence
	}
	if segment.StartedAt.IsZero() {
		segment.StartedAt = existing.StartedAt
	}
	if segment.DurationMS <= 0 {
		segment.DurationMS = existing.DurationMS
	}
}

func deriveABRPathMetadata(streamID string) (string, string, string) {
	if idx := strings.LastIndex(streamID, "/video/"); idx >= 0 {
		return streamID[:idx], "video", streamID[idx+len("/video/"):]
	}
	if idx := strings.LastIndex(streamID, "/audio/"); idx >= 0 {
		return streamID[:idx], "audio", streamID[idx+len("/audio/"):]
	}
	return streamID, "unknown", ""
}

func deriveMuxSessionID(fileName, childToken string) string {
	base := strings.TrimSuffix(fileName, filepath.Ext(fileName))
	if childToken != "" {
		for _, suffix := range []string{"_" + childToken + "_init", "_" + childToken} {
			if strings.HasSuffix(base, suffix) {
				return strings.TrimSuffix(base, suffix)
			}
		}
		segSuffix := regexp.MustCompile(`_` + regexp.QuoteMeta(childToken) + `_seg[0-9]+$`)
		if loc := segSuffix.FindStringIndex(base); loc != nil && loc[1] == len(base) {
			return base[:loc[0]]
		}
	}
	if loc := fmp4SegmentRe.FindStringIndex(fileName); loc != nil && loc[1] == len(fileName) {
		return fileName[:loc[0]]
	}
	return base
}

func (u *HLSS3Uploader) inferDurationMS(filePath string) int64 {
	playlistPath := filepath.Join(filepath.Dir(filePath), "index.m3u8")
	if strings.HasSuffix(filepath.Base(filePath), ".mp4") {
		matches, _ := filepath.Glob(filepath.Join(filepath.Dir(filePath), "*_stream.m3u8"))
		for _, candidate := range matches {
			if duration := inferDurationFromPlaylist(candidate, filepath.Base(filePath)); duration > 0 {
				return duration
			}
		}
	}
	return inferDurationFromPlaylist(playlistPath, filepath.Base(filePath))
}

func inferDurationFromPlaylist(playlistPath string, name string) int64 {
	byts, err := os.ReadFile(playlistPath)
	if err != nil {
		return 0
	}

	lines := strings.Split(string(byts), "\n")
	var pendingDuration int64
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if matches := partRe.FindStringSubmatch(line); matches != nil {
			if matches[2] == name {
				return secondsToMS(matches[1])
			}
			continue
		}

		if matches := extinfRe.FindStringSubmatch(line); matches != nil {
			pendingDuration = secondsToMS(matches[1])
			continue
		}

		if line == name || filepath.Base(line) == name {
			return pendingDuration
		}
	}

	return 0
}

func secondsToMS(raw string) int64 {
	seconds, err := strconv.ParseFloat(strings.TrimRight(raw, ","), 64)
	if err != nil {
		return 0
	}
	return int64(seconds * 1000)
}

// waitStable polls the file until its size stops changing, ensuring it is fully written.
func (u *HLSS3Uploader) waitStable(filePath string) bool {
	var lastSize int64 = -1
	for {
		info, err := os.Stat(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				return false
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if info.Size() == lastSize && lastSize > 0 {
			return true
		}
		lastSize = info.Size()

		select {
		case <-time.After(200 * time.Millisecond):
		case <-u.ctx.Done():
			return false
		}
	}
}

// BuildRemoteKey constructs the S3 storage key for a given stream path and file name
// using the configured prefix, ensuring consistency between upload and lookup.
func (u *HLSS3Uploader) BuildRemoteKey(streamPath, fileName string) string {
	prefix := strings.TrimSuffix(u.Config.Prefix, "/")
	if prefix == "" {
		prefix = "live-hls"
	}
	return path.Join(prefix, streamPath, fileName)
}

func (u *HLSS3Uploader) IsUploaded(remoteKey string) bool {
	if _, ok := u.uploadedFiles.Load(remoteKey); ok {
		return true
	}
	if u.Repository == nil {
		return false
	}
	segment, err := u.Repository.GetByStorageKey(remoteKey)
	if err != nil {
		return false
	}
	return segment.Status == models.LiveHLSSegmentStatusUploadedS3
}

func (u *HLSS3Uploader) Presign(remoteKey string) (string, error) {
	if s3Prov, ok := u.provider.(*S3StorageProvider); ok {
		req, err := s3Prov.presignClient.PresignGetObject(context.Background(), &s3.GetObjectInput{
			Bucket: aws.String(s3Prov.bucket),
			Key:    aws.String(remoteKey),
		}, s3.WithPresignExpires(defaultPresignExpires))
		if err != nil {
			return "", err
		}
		return req.URL, nil
	}
	return "", fmt.Errorf("provider does not support presign")
}

func contentTypeForRemoteKey(remoteKey string) string {
	switch strings.ToLower(path.Ext(remoteKey)) {
	case extM3U8:
		return contentTypeM3U8
	case extMP4, extMP:
		return contentTypeMP4
	case extM4S:
		return contentTypeM4S
	case extTS:
		return contentTypeTS
	default:
		return contentTypeDefault
	}
}

func (u *HLSS3Uploader) ProxyObject(ctx context.Context, w http.ResponseWriter, remoteKey string) bool {
	readable, ok := u.provider.(ReadableStorageProvider)
	if !ok {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}

	body, contentType, contentLength, err := readable.GetObject(ctx, remoteKey)
	if err != nil {
		return false
	}
	defer body.Close()

	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	} else {
		w.Header().Set("Content-Type", contentTypeForRemoteKey(remoteKey))
	}
	if contentLength >= 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	}

	if strings.HasSuffix(remoteKey, extM3U8) {
		w.Header().Set("Cache-Control", cacheControlNoCache)
	} else {
		w.Header().Set("Cache-Control", cacheControlImmutableSegment)
	}

	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, body)
	return true
}
