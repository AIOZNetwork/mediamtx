package hls

import (
	"strings"
	"testing"

	"github.com/bluenviron/mediamtx/internal/conf"
)

func TestRenderABRMasterPlaylistSharedAudioNestedRenditions(t *testing.T) {
	playlist := string(renderABRMasterPlaylist([]conf.HLSTranscodingRendition{
		{Name: "1080", Width: 1920, Height: 1080, VideoBitrate: "6000k"},
		{Name: "720", Width: 1280, Height: 720, VideoBitrate: "3000k"},
		{Name: "480", Width: 854, Height: 480, VideoBitrate: "1200k"},
	}))

	for _, expected := range []string{
		"#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-INDEPENDENT-SEGMENTS\n",
		`#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="main-audio",NAME="Main",LANGUAGE="und",DEFAULT=YES,AUTOSELECT=YES,CHANNELS="2",URI="audio/main/index.m3u8"`,
		`#EXT-X-STREAM-INF:BANDWIDTH=6128000,AVERAGE-BANDWIDTH=6128000,RESOLUTION=1920x1080,FRAME-RATE=30.000,VIDEO-RANGE=SDR,CODECS="avc1.640028,mp4a.40.2",AUDIO="main-audio",CLOSED-CAPTIONS=NONE`,
		"video/1080/index.m3u8",
		"video/720/index.m3u8",
		"video/480/index.m3u8",
	} {
		if !strings.Contains(playlist, expected) {
			t.Fatalf("master playlist missing %q:\n%s", expected, playlist)
		}
	}
}

func TestShouldRenderABRMaster(t *testing.T) {
	pathConf := &conf.Path{
		HLSTranscoding: true,
		HLSTranscodingRenditions: []conf.HLSTranscodingRendition{
			{Name: "720", Width: 1280, Height: 720, VideoBitrate: "3000k"},
		},
	}

	if !shouldRenderABRMaster("cam_1", pathConf) {
		t.Fatalf("expected base path to render master even with underscore")
	}
	if shouldRenderABRMaster("cam_1/video/720", pathConf) {
		t.Fatalf("expected video child path to skip master")
	}
	if shouldRenderABRMaster("cam_1/audio/main", pathConf) {
		t.Fatalf("expected audio child path to skip master")
	}
}

func TestABRChildPathHelpers(t *testing.T) {
	for _, ca := range []struct {
		path     string
		isChild  bool
		basePath string
	}{
		{"cam_1", false, "cam_1"},
		{"cam_1/video/480", true, "cam_1"},
		{"cam_1/audio/main", true, "cam_1"},
		{"nested/cam_1/video/720", true, "nested/cam_1"},
	} {
		if isABRChildPlaylistPath(ca.path) != ca.isChild {
			t.Fatalf("unexpected child detection for %q", ca.path)
		}
		if abrBasePath(ca.path) != ca.basePath {
			t.Fatalf("unexpected base path for %q: %q", ca.path, abrBasePath(ca.path))
		}
	}
}
