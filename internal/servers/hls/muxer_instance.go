package hls

import (
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/bluenviron/gohlslib/v2"
	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/defs"
	"github.com/bluenviron/mediamtx/internal/hlss3uploader"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/protocols/hls"
	"github.com/bluenviron/mediamtx/internal/stream"
	"github.com/gin-gonic/gin"
)

type muxerInstance struct {
	variant         conf.HLSVariant
	segmentCount    int
	segmentDuration conf.Duration
	partDuration    conf.Duration
	segmentMaxSize  conf.StringSize
	directory       string
	uploadConfig    *MuxerUploadConfig
	pathName        string
	stream          *stream.Stream
	bytesSent       *uint64
	parent          logger.Writer
	streamKey       string

	hmuxer      *gohlslib.Muxer
	hlsUploader *hlss3uploader.HLSS3Uploader
}

func (mi *muxerInstance) initialize() error {
	var muxerDirectory string
	if mi.directory != "" {
		muxerDirectory = filepath.Join(mi.directory, mi.pathName)
		if err := os.MkdirAll(muxerDirectory, 0o755); err != nil {
			return err
		}
	}

	if muxerDirectory != "" && mi.uploadConfig != nil {
		mi.hlsUploader = mi.uploadConfig.NewUploader(muxerDirectory, mi.pathName, mi.streamKey, mi)
		if err := mi.hlsUploader.Initialize(); err != nil {
			mi.Log(logger.Warn, "failed to initialize muxer HLS uploader: %v", err)
			mi.hlsUploader = nil
		}
	}

	mi.hmuxer = &gohlslib.Muxer{
		Variant:            gohlslib.MuxerVariant(mi.variant),
		SegmentCount:       mi.segmentCount,
		SegmentMinDuration: time.Duration(mi.segmentDuration),
		PartMinDuration:    time.Duration(mi.partDuration),
		SegmentMaxSize:     uint64(mi.segmentMaxSize),
		Directory:          muxerDirectory,
		OnEncodeError: func(err error) {
			mi.Log(logger.Warn, err.Error())
		},
	}

	err := hls.FromStream(mi.stream, mi, mi.hmuxer)
	if err != nil {
		if mi.hlsUploader != nil {
			mi.hlsUploader.Close()
			mi.hlsUploader = nil
		}
		return err
	}

	err = mi.hmuxer.Start()
	if err != nil {
		mi.stream.RemoveReader(mi)
		if mi.hlsUploader != nil {
			mi.hlsUploader.Close()
			mi.hlsUploader = nil
		}
		return err
	}

	mi.Log(logger.Info, "is converting into HLS, %s",
		defs.FormatsInfo(mi.stream.ReaderFormats(mi)))

	mi.stream.StartReader(mi)

	return nil
}

// Log implements logger.Writer.
func (mi *muxerInstance) Log(level logger.Level, format string, args ...interface{}) {
	mi.parent.Log(level, format, args...)
}

func (mi *muxerInstance) close() {
	mi.stream.RemoveReader(mi)

	// 1. Stop the uploader FIRST — flush pending uploads while local files still exist
	if mi.hlsUploader != nil {
		mi.hlsUploader.Close()
		mi.hlsUploader = nil
	}

	// 2. Close the HLS muxer (gohlslib) — stops writing new segments
	if mi.hmuxer != nil {
		mi.hmuxer.Close()
	}

	// 3. Delete local directory last
	if mi.hmuxer != nil && mi.hmuxer.Directory != "" {
		os.RemoveAll(mi.hmuxer.Directory)
	}
}

func (mi *muxerInstance) errorChan() chan error {
	return mi.stream.ReaderError(mi)
}

func (mi *muxerInstance) localSegmentAvailable(fileName string) bool {
	if mi.directory == "" {
		return true // Fallback to hmuxer if RAM-based
	}
	localPath := filepath.Join(mi.directory, mi.pathName, fileName)
	_, err := os.Stat(localPath)
	return !os.IsNotExist(err)
}

func isHLSSegment(fileName string) bool {
	return strings.HasSuffix(fileName, ".mp4") ||
		strings.HasSuffix(fileName, ".ts") ||
		strings.HasSuffix(fileName, ".m4s") ||
		strings.HasSuffix(fileName, ".mp")
}

func (mi *muxerInstance) handleRequest(ctx *gin.Context) {
	fileName := path.Base(ctx.Request.URL.Path)

	if strings.HasSuffix(fileName, ".m3u8") || strings.HasSuffix(fileName, "_init.mp4") {
		mi.hmuxer.Handle(ctx.Writer, ctx.Request)
		return
	}

	w := &responseWriterWithCounter{
		ResponseWriter: ctx.Writer,
		bytesSent:      mi.bytesSent,
		statusCode:     200,
	}

	if !isHLSSegment(fileName) {
		mi.hmuxer.Handle(w, ctx.Request)
		return
	}

	if mi.localSegmentAvailable(fileName) {
		mi.hmuxer.Handle(w, ctx.Request)
		return
	}

	if mi.hlsUploader == nil {
		ctx.Status(http.StatusNotFound)
		return
	}

	remoteKey := path.Join("live-hls", mi.pathName, fileName)
	if !mi.hlsUploader.IsUploaded(remoteKey) {
		ctx.Status(http.StatusNotFound)
		return
	}

	url, err := mi.hlsUploader.Presign(remoteKey)
	if err != nil {
		ctx.Status(http.StatusBadGateway)
		return
	}

	mi.Log(logger.Info, "[HLS Resolver] Local miss for %s. S3 fallback successful, redirecting to remote.", fileName)
	ctx.Redirect(http.StatusFound, url)
}
