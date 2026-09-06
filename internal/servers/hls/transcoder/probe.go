package transcoder

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// SourceInfo contains probed source stream information.
type SourceInfo struct {
	Width      int
	Height     int
	FPS        float64
	VideoCodec string // e.g. "h264", "hevc"
	AudioCodec string // e.g. "aac", "opus"
	Profile    string // e.g. "High", "Main"
	Level      string // e.g. "4.0", "3.1"
}

type ffprobeOutput struct {
	Streams []ffprobeStream `json:"streams"`
}

type ffprobeStream struct {
	CodecType  string `json:"codec_type"`
	CodecName  string `json:"codec_name"`
	Width      int    `json:"width"`
	Height     int    `json:"height"`
	Profile    string `json:"profile"`
	Level      int    `json:"level"`
	RFrameRate string `json:"r_frame_rate"`
}

const (
	defaultProbeTimeout  = 1500 * time.Millisecond
	defaultRTSPTransport = "tcp"
	ffprobeOutputFormat  = "json"
	ffprobeLogLevel      = "quiet"
	codecTypeVideo       = "video"
	codecTypeAudio       = "audio"
)

// ProbeSource runs ffprobe against an RTSP source URL and returns stream info.
// Times out after defaultProbeTimeout.
func ProbeSource(rtspURL string) (*SourceInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultProbeTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ffprobe",
		"-v", ffprobeLogLevel,
		"-print_format", ffprobeOutputFormat,
		"-show_streams",
		"-analyzeduration", "500000",
		"-probesize", "500000",
		"-fflags", "nobuffer",
		"-rtsp_transport", defaultRTSPTransport,
		rtspURL,
	)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("ffprobe failed: %w", err)
	}

	var probe ffprobeOutput
	if err := json.Unmarshal(out, &probe); err != nil {
		return nil, fmt.Errorf("ffprobe parse failed: %w", err)
	}

	info := &SourceInfo{}
	for _, s := range probe.Streams {
		switch s.CodecType {
		case codecTypeVideo:
			info.Width = s.Width
			info.Height = s.Height
			info.VideoCodec = s.CodecName
			info.Profile = s.Profile
			if s.Level > 0 {
				major := s.Level / 10
				minor := s.Level % 10
				info.Level = fmt.Sprintf("%d.%d", major, minor)
			}
			info.FPS = parseFrameRate(s.RFrameRate)
		case codecTypeAudio:
			info.AudioCodec = s.CodecName
		}
	}
	return info, nil
}

func parseFrameRate(rateStr string) float64 {
	parts := strings.SplitN(rateStr, "/", 2)
	if len(parts) != 2 {
		v, _ := strconv.ParseFloat(rateStr, 64)
		return v
	}
	num, _ := strconv.ParseFloat(parts[0], 64)
	den, _ := strconv.ParseFloat(parts[1], 64)
	if den == 0 {
		return 0
	}
	return num / den
}
