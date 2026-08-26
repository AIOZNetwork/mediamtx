package hls

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/bluenviron/mediamtx/internal/conf"
)

const sharedAudioBandwidth = 128000

func isABROutputPath(pathName string) bool {
	return strings.Contains(pathName, "/video/") || strings.Contains(pathName, "/audio/")
}

func isABRChildPlaylistPath(pathName string) bool {
	return strings.Contains(pathName, "/video/") || strings.Contains(pathName, "/audio/main")
}

func abrBasePath(pathName string) string {
	for _, marker := range []string{"/video/", "/audio/"} {
		if idx := strings.Index(pathName, marker); idx >= 0 {
			return pathName[:idx]
		}
	}
	return pathName
}

func shouldRenderABRMaster(pathName string, pathConf *conf.Path) bool {
	return pathConf != nil && pathConf.HLSTranscoding && len(pathConf.HLSTranscodingRenditions) > 0 && !isABROutputPath(pathName)
}

// renderABRMasterPlaylist assumes the configured FFmpeg ABR ladder uses H.264 High Profile + AAC-LC.
// Exact source codec probing is intentionally avoided here to keep master rendering deterministic.
func renderABRMasterPlaylist(renditions []conf.HLSTranscodingRendition) []byte {
	var b strings.Builder
	b.WriteString("#EXTM3U\n")
	b.WriteString("#EXT-X-VERSION:7\n")
	b.WriteString("#EXT-X-INDEPENDENT-SEGMENTS\n")
	b.WriteString(`#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="main-audio",NAME="Main",LANGUAGE="und",DEFAULT=YES,AUTOSELECT=YES,CHANNELS="2",URI="audio/main/index.m3u8"`)
	b.WriteByte('\n')

	for _, r := range renditions {
		videoBandwidth := parseBitrate(r.VideoBitrate)
		bandwidth := videoBandwidth + sharedAudioBandwidth
		if bandwidth == sharedAudioBandwidth {
			bandwidth = 1_200_000 + sharedAudioBandwidth
		}
		b.WriteString(fmt.Sprintf(
			"#EXT-X-STREAM-INF:BANDWIDTH=%d,AVERAGE-BANDWIDTH=%d,RESOLUTION=%dx%d,FRAME-RATE=30.000,VIDEO-RANGE=SDR,CODECS=\"avc1.640028,mp4a.40.2\",AUDIO=\"main-audio\",CLOSED-CAPTIONS=NONE\n",
			bandwidth, bandwidth, r.Width, r.Height,
		))
		b.WriteString("video/")
		b.WriteString(r.Name)
		b.WriteString("/index.m3u8\n")
	}

	return []byte(b.String())
}

func parseBitrate(raw string) int {
	raw = strings.TrimSpace(strings.ToLower(raw))
	multiplier := 1
	if strings.HasSuffix(raw, "k") {
		multiplier = 1000
		raw = strings.TrimSuffix(raw, "k")
	} else if strings.HasSuffix(raw, "m") {
		multiplier = 1000 * 1000
		raw = strings.TrimSuffix(raw, "m")
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0
	}
	return v * multiplier
}
