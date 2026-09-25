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
	variant                  conf.HLSVariant
	segmentCount             int
	segmentDuration          conf.Duration
	partDuration             conf.Duration
	segmentMaxSize           conf.StringSize
	directory                string
	uploadConfig             *MuxerUploadConfig
	pathConf                 *conf.Path
	hlsTranscodingRenditions []conf.HLSTranscodingRendition
	pathName                 string
	stream                   *stream.Stream
	bytesSent                *uint64
	parent                   logger.Writer
	streamKey                string

	hmuxer      *gohlslib.Muxer
	hlsUploader *hlss3uploader.HLSS3Uploader
}

func isOriginalPath(pathName string) bool {
	return strings.HasSuffix(pathName, "/original") ||
		strings.HasSuffix(pathName, "/video/original") ||
		strings.HasSuffix(pathName, "/audio/original")
}

func (mi *muxerInstance) effectiveVariant() conf.HLSVariant {
	if mi.pathConf != nil && mi.pathConf.HLSTranscoding {
		// Only original stream (root stream, or child original path) uses Low-Latency HLS.
		// Transcoded renditions (1080, 720, 480) use regular segment-based HLS (fMP4).
		if isABROutputPath(mi.pathName) && !isOriginalPath(mi.pathName) {
			if mi.variant == conf.HLSVariant(gohlslib.MuxerVariantMPEGTS) {
				return conf.HLSVariant(gohlslib.MuxerVariantMPEGTS)
			}
			return conf.HLSVariant(gohlslib.MuxerVariantFMP4)
		}
	}
	return mi.variant
}

func (mi *muxerInstance) shouldUploadToProvider() bool {
	if mi.pathConf == nil || !mi.pathConf.HLSTranscoding {
		return true
	}

	// When transcoding is enabled:
	// "original dùng ll-hls và không lưu vào provider, và 3 chất lượng khác từ config sẽ được transcoding và lưu vào provider"
	// 1. Root stream (e.g. <stream_id>) is the original incoming stream -> DO NOT upload
	if !isABROutputPath(mi.pathName) {
		return false
	}

	// 2. original is the original quality (LL-HLS live only) -> DO NOT upload
	if isOriginalPath(mi.pathName) {
		return false
	}

	// 3. Transcoded renditions (1080, 720, 480) -> DO upload
	return true
}

func (mi *muxerInstance) initialize() error {
	mi.variant = mi.effectiveVariant()

	var muxerDirectory string
	if mi.directory != "" {
		muxerDir := filepath.Join(mi.directory, mi.pathName)
		if err := os.MkdirAll(muxerDir, 0o755); err != nil {
			return err
		}
		muxerDirectory = muxerDir
	}

	if muxerDirectory != "" && mi.uploadConfig != nil && mi.shouldUploadToProvider() {
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

	// 1. Close the HLS muxer (gohlslib) FIRST — flush last segment and #EXT-X-ENDLIST to disk
	if mi.hmuxer != nil {
		mi.hmuxer.Close()
	}

	// 2. Stop the uploader and flush all remaining files flushed by the muxer before cleanup
	if mi.hlsUploader != nil {
		mi.hlsUploader.FlushAndClose()
		mi.hlsUploader = nil
	}

	// 3. Delete local directory last after all files are safely uploaded to storage
	if mi.hmuxer != nil && mi.hmuxer.Directory != "" {
		os.RemoveAll(mi.hmuxer.Directory)
	}
}

func (mi *muxerInstance) errorChan() chan error {
	return mi.stream.ReaderError(mi)
}

func (mi *muxerInstance) localSegmentAvailable(fileName string) bool {
	if mi.directory == "" || mi.variant == conf.HLSVariant(gohlslib.MuxerVariantLowLatency) {
		return true // Fallback to hmuxer if RAM-based or LL-HLS
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

func (mi *muxerInstance) hasAudio() bool {
	if mi.stream != nil && mi.stream.Desc() != nil {
		for _, media := range mi.stream.Desc().Medias {
			for _, forma := range media.Formats {
				switch forma.Codec() {
				case "MPEG-4 Audio", "Opus", "LPCM", "Vorbis":
					return true
				}
			}
		}
	}
	if mi.hmuxer != nil {
		for _, track := range mi.hmuxer.Tracks {
			if !track.Codec.IsVideo() {
				return true
			}
		}
	}
	return false
}

func (mi *muxerInstance) primaryVideoPlaylist() string {
	if mi.directory != "" {
		muxerDir := filepath.Join(mi.directory, mi.pathName)
		matches, _ := filepath.Glob(filepath.Join(muxerDir, "*video*_stream.m3u8"))
		if len(matches) > 0 {
			return filepath.Base(matches[0])
		}
		matches, _ = filepath.Glob(filepath.Join(muxerDir, "*_stream.m3u8"))
		for _, m := range matches {
			base := filepath.Base(m)
			if strings.Contains(base, "video") || strings.Contains(base, "main") {
				return base
			}
		}
		if len(matches) > 0 {
			return filepath.Base(matches[0])
		}
	}
	if mi.variant == conf.HLSVariant(gohlslib.MuxerVariantMPEGTS) {
		return "main_stream.m3u8"
	}
	return "video1_stream.m3u8"
}

func (mi *muxerInstance) isMediaPlaylistReady() bool {
	// Low-Latency HLS operates in RAM and never writes playlist files to disk.
	if mi.directory == "" || mi.variant == conf.HLSVariant(gohlslib.MuxerVariantLowLatency) {
		return true
	}
	muxerDir := filepath.Join(mi.directory, mi.pathName)
	matches, _ := filepath.Glob(filepath.Join(muxerDir, "*_stream.m3u8"))
	return len(matches) > 0
}

func (mi *muxerInstance) handleRequest(ctx *gin.Context) {
	// For Low-Latency HLS, all playlists, init files, parts, and segments are managed
	// entirely in RAM by gohlslib. Pass them directly to hmuxer without disk checks.
	if mi.variant == conf.HLSVariant(gohlslib.MuxerVariantLowLatency) {
		mi.hmuxer.Handle(ctx.Writer, ctx.Request)
		return
	}

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

	remoteKey := mi.hlsUploader.BuildRemoteKey(mi.pathName, fileName)
	if !mi.hlsUploader.IsUploaded(remoteKey) {
		ctx.Status(http.StatusNotFound)
		return
	}

	mi.Log(logger.Info, "[HLS Resolver] Local miss for %s. Proxying from remote.", fileName)
	if mi.hlsUploader.ProxyObject(ctx.Request.Context(), ctx.Writer, remoteKey) {
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
