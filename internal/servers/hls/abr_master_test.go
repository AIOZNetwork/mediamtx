package hls

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bluenviron/gohlslib/v2"
	"github.com/bluenviron/mediamtx/internal/conf"
)

func TestRenderABRMasterPlaylistSharedAudioNestedRenditions(t *testing.T) {
	playlist := string(renderABRMasterPlaylist([]conf.HLSTranscodingRendition{
		{Name: "1080", Width: 1920, Height: 1080, VideoBitrate: "6000k"},
		{Name: "720", Width: 1280, Height: 720, VideoBitrate: "3000k"},
		{Name: "480", Width: 854, Height: 480, VideoBitrate: "1200k"},
	}, ""))

	if strings.Contains(playlist, "#EXT-X-MEDIA:TYPE=AUDIO") {
		t.Fatalf("expected master playlist to not contain separate #EXT-X-MEDIA:TYPE=AUDIO tags:\n%s", playlist)
	}

	for _, expected := range []string{
		"#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-INDEPENDENT-SEGMENTS\n",
		`NAME="original"`,
		"original/index.m3u8",
		`NAME="1080p"`,
		`NAME="720p"`,
		`NAME="480p"`,
		`RESOLUTION=1920x1080`,
		`CODECS="avc1.640028,mp4a.40.2"`,
		"1080/index.m3u8",
		"720/index.m3u8",
		"480/index.m3u8",
	} {
		if !strings.Contains(playlist, expected) {
			t.Fatalf("master playlist missing %q:\n%s", expected, playlist)
		}
	}

	lines := strings.Split(playlist, "\n")
	for i, line := range lines {
		if strings.Contains(line, `NAME="original"`) {
			if strings.Contains(line, `CODECS=`) {
				t.Fatalf("original stream should not advertise transcoder CODECS:\n%s", playlist)
			}
			if i+1 >= len(lines) || lines[i+1] != "original/index.m3u8" {
				t.Fatalf("original STREAM-INF not followed by original URI:\n%s", playlist)
			}
			return
		}
	}
	t.Fatalf("missing original stream info:\n%s", playlist)
}

func TestHLSPlaceholderPlaylist(t *testing.T) {
	playlist := string(hlsPlaceholderPlaylist)
	for _, expected := range []string{
		"#EXTM3U\n",
		"#EXT-X-VERSION:7\n",
		"#EXT-X-TARGETDURATION:2\n",
		"#EXT-X-MEDIA-SEQUENCE:0\n",
	} {
		if !strings.Contains(playlist, expected) {
			t.Fatalf("placeholder playlist missing %q:\n%s", expected, playlist)
		}
	}
}

func TestRenderABRMasterPlaylistUsesEffectiveRenditions(t *testing.T) {
	pathConf := &conf.Path{
		HLSTranscoding: true,
		HLSTranscodingRenditions: []conf.HLSTranscodingRendition{
			{Name: "1080", Width: 1920, Height: 1080, VideoBitrate: "6000k"},
			{Name: "720", Width: 1280, Height: 720, VideoBitrate: "3000k"},
			{Name: "480", Width: 854, Height: 480, VideoBitrate: "1200k"},
		},
	}
	mi := &muxerInstance{
		hlsTranscodingRenditions: []conf.HLSTranscodingRendition{
			{Name: "720", Width: 1280, Height: 720, VideoBitrate: "3000k"},
			{Name: "480", Width: 854, Height: 480, VideoBitrate: "1200k"},
		},
	}

	playlist := string(renderABRMasterPlaylist(masterPlaylistRenditions(pathConf, mi), ""))

	for _, expected := range []string{
		`NAME="original"`,
		"original/index.m3u8",
		`NAME="720p"`,
		"720/index.m3u8",
		`NAME="480p"`,
		"480/index.m3u8",
	} {
		if !strings.Contains(playlist, expected) {
			t.Fatalf("master playlist missing %q:\n%s", expected, playlist)
		}
	}
	for _, unexpected := range []string{
		`NAME="1080p"`,
		"1080/index.m3u8",
		`RESOLUTION=1920x1080`,
	} {
		if strings.Contains(playlist, unexpected) {
			t.Fatalf("master playlist unexpectedly contains %q:\n%s", unexpected, playlist)
		}
	}
}

func TestABRChildRenditionAdvertisedUsesEffectiveRenditions(t *testing.T) {
	effective := []conf.HLSTranscodingRendition{
		{Name: "720", Width: 1280, Height: 720, VideoBitrate: "3000k"},
		{Name: "480", Width: 854, Height: 480, VideoBitrate: "1200k"},
	}

	for _, pathName := range []string{"stream1/720", "stream1/480", "stream1/video/720"} {
		if !abrChildRenditionAdvertised(pathName, effective) {
			t.Fatalf("expected %q to be advertised", pathName)
		}
	}

	for _, pathName := range []string{"stream1/1080", "stream1/video/1080"} {
		if abrChildRenditionAdvertised(pathName, effective) {
			t.Fatalf("expected %q to be filtered out", pathName)
		}
	}

	if !abrChildRenditionAdvertised("stream1/original", effective) {
		t.Fatalf("original path should not be treated as a filtered rendition")
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
	if shouldRenderABRMaster("cam_1/original", pathConf) {
		t.Fatalf("expected original child path to skip master")
	}
	if shouldRenderABRMaster("cam_1/720", pathConf) {
		t.Fatalf("expected 720 child path to skip master")
	}
	if shouldRenderABRMaster("cam_1/video/720", pathConf) {
		t.Fatalf("expected video child path to skip master")
	}
	if shouldRenderABRMaster("cam_1/audio/main", pathConf) {
		t.Fatalf("expected audio child path to skip master")
	}
	if shouldRenderABRMaster("cam_1/video/source", pathConf) {
		t.Fatalf("expected source child path to skip master")
	}
}

func TestABRChildPathHelpers(t *testing.T) {
	for _, ca := range []struct {
		path     string
		isChild  bool
		basePath string
	}{
		{"cam_1", false, "cam_1"},
		{"cam_1/original", true, "cam_1"},
		{"cam_1/1080", true, "cam_1"},
		{"cam_1/720", true, "cam_1"},
		{"cam_1/480", true, "cam_1"},
		{"cam_1/video/480", true, "cam_1"},
		{"cam_1/audio/main", true, "cam_1"},
		{"cam_1/video/source", true, "cam_1"},
		{"nested/cam_1/video/720", true, "nested/cam_1"},
		{"nested/cam_1/video/source", true, "nested/cam_1"},
		{"nested/cam_1/original", true, "nested/cam_1"},
		{"nested/cam_1/720", true, "nested/cam_1"},
	} {
		if isABRChildPlaylistPath(ca.path) != ca.isChild {
			t.Fatalf("unexpected child detection for %q", ca.path)
		}
		if abrBasePath(ca.path) != ca.basePath {
			t.Fatalf("unexpected base path for %q: %q", ca.path, abrBasePath(ca.path))
		}
	}
}

func TestCodecStringFromInfoAVCAndFallback(t *testing.T) {
	if got := codecStringFromInfo("libx264", "High", "4.0", "aac"); got != "avc1.640028,mp4a.40.2" {
		t.Fatalf("unexpected high 4.0 codec string %q", got)
	}
	if got := codecStringFromInfo("", "", "", ""); got != "avc1.640028,mp4a.40.2" {
		t.Fatalf("unexpected default codec string %q", got)
	}
	if got := avcCodecString("High", "not-a-level"); got != "avc1.640028" {
		t.Fatalf("unexpected malformed AVC fallback %q", got)
	}
}

func TestShouldUploadToProvider(t *testing.T) {
	transcodingConf := &conf.Path{
		HLSTranscoding: true,
		HLSTranscodingRenditions: []conf.HLSTranscodingRendition{
			{Name: "1080", Width: 1920, Height: 1080, VideoBitrate: "6000k"},
			{Name: "720", Width: 1280, Height: 720, VideoBitrate: "3000k"},
			{Name: "480", Width: 854, Height: 480, VideoBitrate: "1200k"},
		},
	}

	for _, ca := range []struct {
		pathName     string
		shouldUpload bool
	}{
		{"stream1", false},                // root original stream: no upload
		{"stream1/original", false},       // original: no upload (LL-HLS live only)
		{"stream1/video/original", false}, // video/original compat: no upload
		{"stream1/1080", true},            // transcoded 1080: upload
		{"stream1/720", true},             // transcoded 720: upload
		{"stream1/480", true},             // transcoded 480: upload
		{"stream1/video/1080", true},      // video/1080 compat: upload
	} {
		mi := &muxerInstance{
			pathName: ca.pathName,
			pathConf: transcodingConf,
		}
		if got := mi.shouldUploadToProvider(); got != ca.shouldUpload {
			t.Fatalf("expected shouldUploadToProvider(%q) = %v, got %v", ca.pathName, ca.shouldUpload, got)
		}
	}

	// When transcoding is disabled, root stream uploads
	noTranscodeConf := &conf.Path{HLSTranscoding: false}
	mi := &muxerInstance{pathName: "stream1", pathConf: noTranscodeConf}
	if !mi.shouldUploadToProvider() {
		t.Fatalf("expected shouldUploadToProvider to be true when transcoding is disabled")
	}
}

func TestMuxerInstanceEffectiveVariant(t *testing.T) {
	transcodingConf := &conf.Path{
		HLSTranscoding: true,
	}

	for _, ca := range []struct {
		pathName string
		expected conf.HLSVariant
	}{
		{"stream1", conf.HLSVariant(gohlslib.MuxerVariantLowLatency)},                // root stream: LL-HLS
		{"stream1/original", conf.HLSVariant(gohlslib.MuxerVariantLowLatency)},       // original: LL-HLS
		{"stream1/video/original", conf.HLSVariant(gohlslib.MuxerVariantLowLatency)}, // video/original compat: LL-HLS
		{"stream1/1080", conf.HLSVariant(gohlslib.MuxerVariantFMP4)},                 // transcoded 1080: fMP4 segments
		{"stream1/720", conf.HLSVariant(gohlslib.MuxerVariantFMP4)},                  // transcoded 720: fMP4 segments
		{"stream1/480", conf.HLSVariant(gohlslib.MuxerVariantFMP4)},                  // transcoded 480: fMP4 segments
		{"stream1/video/1080", conf.HLSVariant(gohlslib.MuxerVariantFMP4)},           // video/1080 compat: fMP4 segments
	} {
		mi := &muxerInstance{
			variant:  conf.HLSVariant(gohlslib.MuxerVariantLowLatency),
			pathName: ca.pathName,
			pathConf: transcodingConf,
		}
		if got := mi.effectiveVariant(); got != ca.expected {
			t.Fatalf("expected effectiveVariant(%q) = %v, got %v", ca.pathName, ca.expected, got)
		}
	}
}

func TestMuxerInstanceIsMediaPlaylistReady(t *testing.T) {
	tmpDir := t.TempDir()
	pathName := "stream1/video/480"
	muxerDir := filepath.Join(tmpDir, pathName)
	if err := os.MkdirAll(muxerDir, 0o755); err != nil {
		t.Fatalf("failed to create muxer dir: %v", err)
	}

	mi := &muxerInstance{
		variant:   conf.HLSVariant(gohlslib.MuxerVariantFMP4),
		directory: tmpDir,
		pathName:  pathName,
	}

	// Empty dir for FMP4: not ready
	if mi.isMediaPlaylistReady() {
		t.Fatalf("expected isMediaPlaylistReady to be false for empty directory in FMP4 mode")
	}

	// Low-Latency mode: always ready (in RAM)
	miLL := &muxerInstance{
		variant:   conf.HLSVariant(gohlslib.MuxerVariantLowLatency),
		directory: tmpDir,
		pathName:  pathName,
	}
	if !miLL.isMediaPlaylistReady() {
		t.Fatalf("expected isMediaPlaylistReady to be true for LowLatency mode")
	}

	// In FMP4 HLS, init segment file alone does NOT mean media playlist is ready
	initFile := filepath.Join(muxerDir, "video1_init.mp4")
	if err := os.WriteFile(initFile, []byte("ftyp"), 0o644); err != nil {
		t.Fatalf("failed to write init file: %v", err)
	}
	if mi.isMediaPlaylistReady() {
		t.Fatalf("expected isMediaPlaylistReady to be false when only init file exists")
	}

	// In FMP4 HLS, media playlist is ready only when *_stream.m3u8 is written
	streamFile := filepath.Join(muxerDir, "video1_stream.m3u8")
	if err := os.WriteFile(streamFile, []byte("#EXTM3U\n"), 0o644); err != nil {
		t.Fatalf("failed to write stream file: %v", err)
	}
	if !mi.isMediaPlaylistReady() {
		t.Fatalf("expected isMediaPlaylistReady to be true when stream playlist file exists")
	}
}

func TestRenderABRMasterPlaylistWithAudio(t *testing.T) {
	playlist := string(renderABRMasterPlaylist([]conf.HLSTranscodingRendition{
		{Name: "1080", Width: 1920, Height: 1080, VideoBitrate: "6000k"},
		{Name: "720", Width: 1280, Height: 720, VideoBitrate: "3000k"},
		{Name: "480", Width: 854, Height: 480, VideoBitrate: "1200k"},
	}, "", true))

	t.Logf("RENDERED PLAYLIST:\n%s", playlist)

	for _, expected := range []string{
		"#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-INDEPENDENT-SEGMENTS\n",
		`#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="audio",DEFAULT=YES,AUTOSELECT=YES,URI="original/audio2_stream.m3u8"`,
		`AUDIO="audio"`,
		"original/index.m3u8",
		"1080/index.m3u8",
		"720/index.m3u8",
		"480/index.m3u8",
	} {
		if !strings.Contains(playlist, expected) {
			t.Fatalf("master playlist missing %q:\n%s", expected, playlist)
		}
	}
}

func TestMuxerInstancePrimaryVideoPlaylist(t *testing.T) {
	tmpDir := t.TempDir()
	pathName := "stream1/480"
	muxerDir := filepath.Join(tmpDir, pathName)
	if err := os.MkdirAll(muxerDir, 0o755); err != nil {
		t.Fatalf("failed to create muxer dir: %v", err)
	}

	mi := &muxerInstance{
		variant:   conf.HLSVariant(gohlslib.MuxerVariantFMP4),
		directory: tmpDir,
		pathName:  pathName,
	}

	// When no files exist yet, defaults to video1_stream.m3u8
	if got := mi.primaryVideoPlaylist(); got != "video1_stream.m3u8" {
		t.Fatalf("expected video1_stream.m3u8, got %q", got)
	}

	// Create audio2_stream.m3u8 and video1_stream.m3u8
	_ = os.WriteFile(filepath.Join(muxerDir, "audio2_stream.m3u8"), []byte("#EXTM3U\n"), 0o644)
	_ = os.WriteFile(filepath.Join(muxerDir, "video1_stream.m3u8"), []byte("#EXTM3U\n"), 0o644)

	// Must select video1_stream.m3u8, NOT audio2_stream.m3u8
	if got := mi.primaryVideoPlaylist(); got != "video1_stream.m3u8" {
		t.Fatalf("expected video1_stream.m3u8, got %q", got)
	}

	// MPEG-TS variant returns main_stream.m3u8
	miTS := &muxerInstance{
		variant: conf.HLSVariant(gohlslib.MuxerVariantMPEGTS),
	}
	if got := miTS.primaryVideoPlaylist(); got != "main_stream.m3u8" {
		t.Fatalf("expected main_stream.m3u8 for MPEG-TS, got %q", got)
	}
}
