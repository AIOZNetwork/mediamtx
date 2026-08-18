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
	if mi.hmuxer != nil {
		mi.hmuxer.Close()
	}
	if mi.hlsUploader != nil {
		mi.hlsUploader.Close()
		mi.hlsUploader = nil
	}
	if mi.hmuxer != nil && mi.hmuxer.Directory != "" {
		os.RemoveAll(mi.hmuxer.Directory)
	}
}

func (mi *muxerInstance) errorChan() chan error {
	return mi.stream.ReaderError(mi)
}

func (mi *muxerInstance) handleRequest(ctx *gin.Context) {
	fname := path.Base(ctx.Request.URL.Path)

	if isHLSMediaFile(fname) {
		remoteKey := path.Join(
			"live-hls",
			mi.pathName,
			fname,
		)

		if mi.hlsUploader != nil && mi.hlsUploader.IsUploaded(remoteKey) {
			url, err := mi.hlsUploader.Presign(remoteKey)
			if err == nil {
				ctx.Redirect(http.StatusFound, url)
				return
			}
		}
	}

	w := &responseWriterWithCounter{
		ResponseWriter: ctx.Writer,
		bytesSent:      mi.bytesSent,
	}

	mi.hmuxer.Handle(w, ctx.Request)
}

func isHLSMediaFile(name string) bool {
	return strings.HasSuffix(name, ".mp4") ||
		strings.HasSuffix(name, ".ts") ||
		strings.HasSuffix(name, ".mp")
}
