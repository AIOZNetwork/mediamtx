package hlss3uploader

import (
	"context"
	"fmt"
	"io/fs"
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

// Initialize initializes and starts the HLSS3Uploader background service using supplied Config.
func (u *HLSS3Uploader) Initialize() error {
	u.ctx, u.ctxCancel = context.WithCancel(context.Background())
	u.done = make(chan struct{})
	u.taskChan = make(chan string, 1000)
	if u.Config.Workers <= 0 {
		u.Config.Workers = 4
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
		u.Config.Directory = "./input-live"
	}
	if u.Config.Prefix == "" {
		u.Config.Prefix = "live-hls"
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
		<-u.done
	}
	u.wg.Wait()
	if u.provider != nil {
		remotePrefix := path.Join(
			"live-hls",
			u.Config.StreamName,
		)
		if remotePrefix != "" && remotePrefix != "live-hls/" {
			u.provider.DeleteFolder(context.Background(), remotePrefix)
		}
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

	contentType := getContentType(ext)
	segmentRecord := models.LiveHLSSegment{
		StreamID:       streamName,
		SegmentName:    filepath.Base(filePath),
		LocalPath:      filePath,
		StorageBackend: u.provider.Name(),
		StorageKey:     remoteKey,
		DurationMS:     u.inferDurationMS(filePath),
		SizeBytes:      info.Size(),
		ContentType:    contentType,
	}
	if segmentRecord.StreamID == "" {
		segmentRecord.StreamID = filepath.Base(u.Config.Directory)
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
		if u.Config.DeleteLocalAfterUpload {
			err := os.Remove(filePath)
			if err == nil {
				deletedAt := time.Now()
				if u.Repository != nil {
					_ = u.Repository.MarkLocalDeleted(segmentRecord.StreamID, segmentRecord.SegmentName, deletedAt)
				}
				u.Log(logger.Info, "uploaded segment %s via %s (key: %s, streamKey: %s) and deleted local file", relPath, u.provider.Name(), remoteKey, u.Config.StreamKey)
			} else if !os.IsNotExist(err) {
				u.Log(logger.Warn, "uploaded segment %s via %s (streamKey: %s) but failed to delete local file: %v", relPath, u.provider.Name(), u.Config.StreamKey, err)
			} else {
				u.Log(logger.Info, "uploaded segment %s via %s (key: %s, streamKey: %s) (local file already deleted)", relPath, u.provider.Name(), remoteKey, u.Config.StreamKey)
			}
		} else {
			u.Log(logger.Info, "uploaded segment %s via %s (key: %s, streamKey: %s)", relPath, u.provider.Name(), remoteKey, u.Config.StreamKey)
		}
	} else {
		u.Log(logger.Info, "uploaded playlist %s via %s (key: %s, streamKey: %s)", relPath, u.provider.Name(), remoteKey, u.Config.StreamKey)
	}
}



func getContentType(ext string) string {
	switch ext {
	case ".m4s":
		return "video/iso.segment"
	case ".mp4":
		return "video/mp4"
	case ".mp":
		return "video/mp4"
	case ".ts":
		return "video/mp2t"
	case ".m3u8":
		return "application/x-mpegURL"
	default:
		return "application/octet-stream"
	}
}

var (
	extinfRe = regexp.MustCompile(`^#EXTINF:([0-9.]+)`)                               // #EXTINF:1.000,
	partRe   = regexp.MustCompile(`^#EXT-X-PART:.*DURATION=([0-9.]+).*URI="([^"]+)"`) // LL-HLS parts
	mapURIRe = regexp.MustCompile(`^#EXT-X-MAP:.*URI="([^"]+)"`)                      // init segment
)

func (u *HLSS3Uploader) inferDurationMS(filePath string) int64 {
	playlistPath := filepath.Join(filepath.Dir(filePath), "index.m3u8")
	byts, err := os.ReadFile(playlistPath)
	if err != nil {
		return 0
	}

	name := filepath.Base(filePath)
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

func (u *HLSS3Uploader) IsUploaded(remoteKey string) bool {
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
		}, s3.WithPresignExpires(15*time.Minute))
		if err != nil {
			return "", err
		}
		return req.URL, nil
	}
	return "", fmt.Errorf("provider does not support presign")
}
