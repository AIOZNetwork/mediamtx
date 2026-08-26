package transcoder

import (
	"strings"
	"testing"

	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/logger"
)

type mockLogger struct {
	t *testing.T
}

func (l *mockLogger) Log(level logger.Level, format string, args ...interface{}) {
	l.t.Logf(format, args...)
}

func TestTranscoderInit(t *testing.T) {
	cfg := &conf.Path{
		HLSTranscoding: true,
		HLSTranscodingRenditions: []conf.HLSTranscodingRendition{
			{Name: "720", Width: 1280, Height: 720, VideoBitrate: "3000k"},
		},
	}

	l := &mockLogger{t: t}
	tr := NewTranscoder(cfg, "test_stream", l, ":8554")

	if tr.StreamID != "test_stream" {
		t.Errorf("expected streamID test_stream, got %s", tr.StreamID)
	}
}

func TestFFmpegBuildArgsSharedAudioNestedOutputs(t *testing.T) {
	cfg := &conf.Path{
		HLSTranscoding: true,
		HLSTranscodingRenditions: []conf.HLSTranscodingRendition{
			{Name: "1080", Width: 1920, Height: 1080, VideoBitrate: "6000k"},
			{Name: "720", Width: 1280, Height: 720, VideoBitrate: "3000k"},
			{Name: "480", Width: 854, Height: 480, VideoBitrate: "1200k"},
		},
		HLSTranscodingVideoCodec: "libx264",
		HLSTranscodingAudioCodec: "aac",
		HLSTranscodingPreset:     "veryfast",
	}

	tr := NewFFmpegTranscoder(cfg, "cam1", &mockLogger{t: t}, ":8554")
	args := strings.Join(tr.BuildArgs(), " ")

	for _, expected := range []string{
		"-i rtsp://127.0.0.1:8554/cam1",
		"fps=30,setpts=PTS-STARTPTS,split=3[v1080in][v720in][v480in]",
		"scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2[out1080]",
		"scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2[out720]",
		"scale=854:480:force_original_aspect_ratio=decrease,pad=854:480:(ow-iw)/2:(oh-ih)/2[out480]",
		"-map [out1080] -an -c:v libx264 -pix_fmt yuv420p -b:v 6000k",
		"-g 60 -keyint_min 60 -sc_threshold 0 -x264-params scenecut=0:open_gop=0",
		"rtsp://127.0.0.1:8554/cam1/video/1080",
		"rtsp://127.0.0.1:8554/cam1/video/720",
		"rtsp://127.0.0.1:8554/cam1/video/480",
		"-map 0:a:0? -vn -af aresample=async=1:first_pts=0 -c:a aac -b:a 128k -ar 48000 -ac 2",
		"rtsp://127.0.0.1:8554/cam1/audio/main",
	} {
		if !strings.Contains(args, expected) {
			t.Fatalf("args missing %q:\n%s", expected, args)
		}
	}
}
