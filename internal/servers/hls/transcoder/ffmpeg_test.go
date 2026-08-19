package transcoder

import (
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
