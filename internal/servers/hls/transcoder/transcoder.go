package transcoder

import (
	"fmt"

	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/logger"
)

type Transcoder struct {
	Conf        *conf.Path
	StreamID    string
	Parent      logger.Writer
	rtspAddress string
	ffmpeg      *FFmpegTranscoder
}

func NewTranscoder(cfg *conf.Path, streamID string, parent logger.Writer, rtspAddress string) *Transcoder {
	return &Transcoder{
		Conf:        cfg,
		StreamID:    streamID,
		Parent:      parent,
		rtspAddress: rtspAddress,
	}
}

func (t *Transcoder) Start() error {
	if !t.Conf.HLSTranscoding || len(t.Conf.HLSTranscodingRenditions) == 0 {
		return fmt.Errorf("transcoding not enabled or no renditions configured")
	}

	t.ffmpeg = NewFFmpegTranscoder(t.Conf, t.StreamID, t.Parent, t.rtspAddress)
	return t.ffmpeg.Start()
}

func (t *Transcoder) Stop() {
	if t.ffmpeg != nil {
		t.ffmpeg.Stop()
	}
}
