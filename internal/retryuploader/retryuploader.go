package retryuploader

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/grpc_service"
	"github.com/bluenviron/mediamtx/internal/logger"
)

var timeNow = time.Now

// RetryUploader uploads recording segments to gRPC server after a specified delay.
type RetryUploader struct {
	PathConfs   map[string]*conf.Path
	Parent      logger.Writer
	UploadDelay time.Duration // Delay before uploading segments
	RetryDir    string        // Directory to scan for retry uploads (default: "./retry")

	ctx       context.Context
	ctxCancel func()

	chReloadConf chan map[string]*conf.Path
	done         chan struct{}
}

// Initialize initializes a RetryUploader.
func (u *RetryUploader) Initialize() {
	u.ctx, u.ctxCancel = context.WithCancel(context.Background())
	u.chReloadConf = make(chan map[string]*conf.Path)
	u.done = make(chan struct{})

	if u.UploadDelay == 0 {
		u.UploadDelay = 5 * time.Minute // Default 5 minutes
	}

	if u.RetryDir == "" {
		u.RetryDir = "./retry" // Default retry directory
	}

	go u.run()
}

// Close closes the RetryUploader.
func (u *RetryUploader) Close() {
	u.ctxCancel()
	<-u.done
}

// Log implements logger.Writer.
func (u *RetryUploader) Log(level logger.Level, format string, args ...any) {
	u.Parent.Log(level, "[retry uploader] "+format, args...)
}

// ReloadPathConfs is called by core.Core.
func (u *RetryUploader) ReloadPathConfs(pathConfs map[string]*conf.Path) {
	select {
	case u.chReloadConf <- pathConfs:
	case <-u.ctx.Done():
	}
}

func (u *RetryUploader) run() {
	defer close(u.done)

	u.Log(logger.Info, "retry uploader started, checking every %v", u.uploadInterval())

	ticker := time.NewTicker(u.uploadInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			u.Log(logger.Debug, "running retry upload check")
			u.doRun()

		case cnf := <-u.chReloadConf:
			u.PathConfs = cnf

		case <-u.ctx.Done():
			u.Log(logger.Info, "retry uploader stopped")
			return
		}
	}
}

func (u *RetryUploader) uploadInterval() time.Duration {
	return 1 * time.Minute
}

func (u *RetryUploader) doRun() {
	u.Log(logger.Debug, "scanning retry directory: %s", u.RetryDir)

	now := timeNow()

	err := u.scanRetryDirectory(now)
	if err != nil {
		u.Log(logger.Warn, "failed to scan retry directory: %v", err)
	}
}

func (u *RetryUploader) scanRetryDirectory(now time.Time) error {
	if _, err := os.Stat(u.RetryDir); os.IsNotExist(err) {
		u.Log(logger.Debug, "retry directory does not exist: %s", u.RetryDir)
		return nil
	}

	u.Log(logger.Debug, "retry directory exists, scanning for files")

	uploadBefore := now.Add(-u.UploadDelay)
	filesFound := 0
	filesUploaded := 0

	err := filepath.WalkDir(u.RetryDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		filesFound++

		info, err := d.Info()
		if err != nil {
			return nil
		}

		// Check if file is old enough to upload
		if info.ModTime().After(uploadBefore) {
			u.Log(logger.Debug, "file %s is too new (modified: %v, upload after: %v)", path, info.ModTime(), uploadBefore)
			return nil // File is too new, skip it
		}

		u.Log(logger.Info, "attempting to upload file: %s", path)

		err = u.uploadSegment(path)
		if err != nil {
			u.Log(logger.Warn, "failed to upload %s: %v", path, err)
		} else {
			u.Log(logger.Info, "successfully uploaded %s", path)
			os.Remove(path)
			filesUploaded++
		}

		return nil
	})

	u.Log(logger.Debug, "scan complete: found %d files, uploaded %d files", filesFound, filesUploaded)

	return err
}

func (u *RetryUploader) uploadSegment(fpath string) error {
	file, err := os.Open(fpath)
	if err != nil {
		return err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}

	fileName := filepath.Base(fpath)
	mediaID := u.extractMediaID(fpath)
	if mediaID == "" {
		u.Log(logger.Warn, "cannot extract mediaID from path: %s", fpath)
		return nil
	}

	ctx, cancel := context.WithTimeout(u.ctx, 5*time.Minute)
	defer cancel()

	err = grpc_service.W3GrpcClient.UploadMedia(ctx, mediaID, fileName, fileInfo.Size(), file)
	if err != nil {
		return err
	}

	return nil
}

func (u *RetryUploader) extractMediaID(fpath string) string {
	// Extract mediaID from path like "./retry/{videoId}/video.mp4"
	// or "retry/{videoId}/video.mp4"
	parts := strings.Split(filepath.Clean(fpath), string(filepath.Separator))

	for i, part := range parts {
		if part == "retry" && i+1 < len(parts) {
			return parts[i+1]
		}
	}

	return ""
}
