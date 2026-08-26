package transcoder

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"strings"

	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/logger"
)

type FFmpegTranscoder struct {
	Conf        *conf.Path
	StreamID    string
	Parent      logger.Writer
	rtspAddress string

	ctx       context.Context
	ctxCancel context.CancelFunc
}

func NewFFmpegTranscoder(cfg *conf.Path, streamID string, parent logger.Writer, rtspAddress string) *FFmpegTranscoder {
	ctx, cancel := context.WithCancel(context.Background())
	return &FFmpegTranscoder{
		Conf:        cfg,
		StreamID:    streamID,
		Parent:      parent,
		rtspAddress: rtspAddress,
		ctx:         ctx,
		ctxCancel:   cancel,
	}
}

func (t *FFmpegTranscoder) Log(level logger.Level, format string, args ...interface{}) {
	t.Parent.Log(level, "[Transcoder %s] "+format, append([]interface{}{t.StreamID}, args...)...)
}

func (t *FFmpegTranscoder) Start() error {
	t.Log(logger.Info, "Starting FFmpeg transcoder for stream %s", t.StreamID)
	args := t.BuildArgs()

	cmd := exec.CommandContext(t.ctx, "ffmpeg", args...)

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			t.Log(logger.Warn, "FFmpeg: %s", scanner.Text())
		}
	}()

	go func() {
		err := cmd.Wait()
		if err != nil && t.ctx.Err() == nil {
			t.Log(logger.Error, "FFmpeg transcoder exited with error: %v", err)
		} else {
			t.Log(logger.Info, "FFmpeg transcoder stopped")
		}
	}()

	return nil
}

// BuildArgs builds FFmpeg arguments without starting the process.
func (t *FFmpegTranscoder) BuildArgs() []string {
	rtspPort := t.rtspPort()
	sourceURL := fmt.Sprintf("rtsp://127.0.0.1:%s/%s", rtspPort, t.StreamID)

	renditions := t.Conf.HLSTranscodingRenditions
	filterGraph := t.videoFilterGraph(renditions)

	args := []string{
		"-hide_banner",
		"-loglevel", "warning",
		"-fflags", "+genpts",
		"-rtsp_transport", "tcp",
		"-i", sourceURL,
		"-filter_complex", filterGraph,
	}

	videoCodec := defaultString(t.Conf.HLSTranscodingVideoCodec, "libx264")
	preset := defaultString(t.Conf.HLSTranscodingPreset, "veryfast")
	for _, r := range renditions {
		outURL := fmt.Sprintf("rtsp://127.0.0.1:%s/%s/video/%s", rtspPort, t.StreamID, r.Name)
		args = append(args,
			"-map", fmt.Sprintf("[out%s]", r.Name),
			"-an",
			"-c:v", videoCodec,
			"-pix_fmt", "yuv420p",
			"-b:v", r.VideoBitrate,
			"-preset", preset,
			"-g", "60",
			"-keyint_min", "60",
			"-sc_threshold", "0",
			"-x264-params", "scenecut=0:open_gop=0",
			"-f", "rtsp",
			"-rtsp_transport", "tcp",
			outURL,
		)
	}

	args = append(args,
		"-map", "0:a:0?",
		"-vn",
		"-af", "aresample=async=1:first_pts=0",
		"-c:a", defaultString(t.Conf.HLSTranscodingAudioCodec, "aac"),
		"-b:a", "128k",
		"-ar", "48000",
		"-ac", "2",
		"-f", "rtsp",
		"-rtsp_transport", "tcp",
		fmt.Sprintf("rtsp://127.0.0.1:%s/%s/audio/main", rtspPort, t.StreamID),
	)

	return args
}

func (t *FFmpegTranscoder) rtspPort() string {
	_, rtspPort, err := net.SplitHostPort(t.rtspAddress)
	if err != nil || rtspPort == "" {
		return "8554"
	}
	return rtspPort
}

func (t *FFmpegTranscoder) videoFilterGraph(renditions []conf.HLSTranscodingRendition) string {
	var splitOuts []string
	for _, r := range renditions {
		splitOuts = append(splitOuts, fmt.Sprintf("[v%sin]", r.Name))
	}

	var b strings.Builder
	b.WriteString("[0:v]fps=30,setpts=PTS-STARTPTS,split=")
	b.WriteString(strconv.Itoa(len(renditions)))
	b.WriteString(strings.Join(splitOuts, ""))
	b.WriteByte(';')

	for _, r := range renditions {
		b.WriteString(fmt.Sprintf(
			"[v%sin]scale=%d:%d:force_original_aspect_ratio=decrease,pad=%d:%d:(ow-iw)/2:(oh-ih)/2[out%s];",
			r.Name, r.Width, r.Height, r.Width, r.Height, r.Name,
		))
	}

	return strings.TrimSuffix(b.String(), ";")
}

func defaultString(v string, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

func (t *FFmpegTranscoder) Stop() {
	t.ctxCancel()
}
