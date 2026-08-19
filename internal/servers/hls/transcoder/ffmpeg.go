package transcoder

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os/exec"
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

	rtspHost, rtspPort, err := net.SplitHostPort(t.rtspAddress)
	if err != nil || rtspPort == "" {
		rtspPort = "8554" // fallback
	}
	if rtspHost == "" || rtspHost == "0.0.0.0" || rtspHost == "[::]" {
		rtspHost = "127.0.0.1"
	}

	sourceURL := fmt.Sprintf("%s://%s:%s/%s", t.Conf.HLSTranscodingInputProto, rtspHost, rtspPort, t.StreamID)
	
	// Create filter graph
	filterGraph := "[0:v]split=" + fmt.Sprintf("%d", len(t.Conf.HLSTranscodingRenditions))
	
	var splitOuts []string
	var aSplitOuts []string
	for _, r := range t.Conf.HLSTranscodingRenditions {
		splitOuts = append(splitOuts, fmt.Sprintf("[v%s]", r.Name))
		aSplitOuts = append(aSplitOuts, fmt.Sprintf("[a%s]", r.Name))
	}
	filterGraph += strings.Join(splitOuts, "") + ";"

	for _, r := range t.Conf.HLSTranscodingRenditions {
		filterGraph += fmt.Sprintf("[v%s]scale=%d:%d[out%s];", r.Name, r.Width, r.Height, r.Name)
	}

	// Add audio filter for timestamp correction and splitting
	filterGraph += fmt.Sprintf("[0:a]asetpts=N/SR/TB,asplit=%d%s", len(t.Conf.HLSTranscodingRenditions), strings.Join(aSplitOuts, ""))

	args := []string{
		"-hide_banner",
		"-loglevel", "warning",
		"-fflags", "+genpts",
		"-rtsp_transport", "tcp",
		"-i", sourceURL,
		"-filter_complex", filterGraph,
	}

	for _, r := range t.Conf.HLSTranscodingRenditions {
		outURL := fmt.Sprintf("%s://%s:%s/%s_%s", t.Conf.HLSTranscodingInputProto, rtspHost, rtspPort, t.StreamID, r.Name)
		
		args = append(args,
			"-map", fmt.Sprintf("[out%s]", r.Name),
			"-map", fmt.Sprintf("[a%s]", r.Name),
			"-c:v", t.Conf.HLSTranscodingVideoCodec,
			"-b:v", r.VideoBitrate,
			"-c:a", t.Conf.HLSTranscodingAudioCodec,
			"-preset", t.Conf.HLSTranscodingPreset,
			"-f", t.Conf.HLSTranscodingInputProto, // rtsp
			"-rtsp_transport", "tcp",
			outURL,
		)
	}

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

func (t *FFmpegTranscoder) Stop() {
	t.ctxCancel()
}
