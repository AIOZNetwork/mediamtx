package hls

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/servers/hls/transcoder"
)

const (
	// Bandwidth defaults (in bps)
	sharedAudioBandwidth          = 128000
	sourceBandwidth               = 8000000
	defaultFallbackVideoBandwidth = 1200000

	// Codec identifiers
	codecLibx264 = "libx264"
	codecH264    = "h264"
	codecAVC     = "avc"
	codecHEVC    = "hevc"
	codecH265    = "h265"
	codecLibx265 = "libx265"
	codecAAC     = "aac"
	codecOpus    = "opus"
	codecLibopus = "libopus"

	// HLS Codecs string standards (RFC 6381)
	defaultAVCCodecString    = "avc1.640028"
	defaultHEVCCodecString   = "hvc1.1.6.L120.B0"
	defaultAACCodecString    = "mp4a.40.2"
	defaultOpusCodecString   = "Opus"
	defaultMasterCodecString = defaultAVCCodecString + "," + defaultAACCodecString

	// H.264 Profile and Level hex standards
	avcProfileBaselineHex = "42"
	avcProfileMainHex     = "4d"
	avcProfileHighHex     = "64"
	avcProfileHigh10Hex   = "6e"
	avcDefaultLevelHex    = "28"

	// H.264 Profile names
	avcProfileNameBaseline            = "baseline"
	avcProfileNameConstrainedBaseline = "constrained baseline"
	avcProfileNameMain                = "main"
	avcProfileNameHigh                = "high"
	avcProfileNameHigh10              = "high 10"

	// Codec format strings
	avcCodecFormat = "avc1.%s00%s"
	codecSeparator = ","

	// Path markers
	pathMarkerVideo = "/video/"
	pathMarkerAudio = "/audio/"
)

func isABRChildPlaylistPath(pathName string) bool {
	if !strings.Contains(pathName, "/") {
		return false
	}
	if strings.Contains(pathName, pathMarkerVideo) || strings.Contains(pathName, pathMarkerAudio) {
		return true
	}
	lastPart := pathName[strings.LastIndex(pathName, "/")+1:]
	if lastPart == "original" || lastPart == "main" {
		return true
	}
	if _, err := strconv.Atoi(strings.TrimSuffix(lastPart, "p")); err == nil {
		return true
	}
	return false
}

func isABROutputPath(pathName string) bool {
	return isABRChildPlaylistPath(pathName)
}


func abrBasePath(pathName string) string {
	if !isABRChildPlaylistPath(pathName) {
		return pathName
	}
	for _, marker := range []string{pathMarkerVideo, pathMarkerAudio} {
		if idx := strings.Index(pathName, marker); idx >= 0 {
			return pathName[:idx]
		}
	}
	if idx := strings.LastIndex(pathName, "/"); idx >= 0 {
		return pathName[:idx]
	}
	return pathName
}

// isConfiguredABRChildPath checks whether pathName is a valid ABR rendition
// child path that is actually enabled in pathConf's transcoding renditions.
// Supports both legacy paths (video/<name>) and flat paths (<name>).
func isConfiguredABRChildPath(pathName string, pathConf *conf.Path) bool {
	if pathConf == nil || !pathConf.HLSTranscoding || !strings.Contains(pathName, "/") {
		return false
	}

	basePath := abrBasePath(pathName)
	childName := strings.TrimPrefix(pathName[len(basePath):], "/")
	// legacy: video/source, audio/main
	if childName == "video/source" || childName == "audio/main" || childName == "original" || childName == "main" {
		return true
	}
	// legacy: video/<renditionName>
	for _, rendition := range pathConf.HLSTranscodingRenditions {
		if childName == "video/"+rendition.Name || childName == rendition.Name {
			return true
		}
	}
	return false
}

func abrChildRenditionName(pathName string) (string, bool) {
	if !isABRChildPlaylistPath(pathName) {
		return "", false
	}
	lastPart := pathName[strings.LastIndex(pathName, "/")+1:]
	if lastPart == "original" || lastPart == "main" {
		return "", false
	}
	name := strings.TrimSuffix(lastPart, "p")
	if _, err := strconv.Atoi(name); err != nil {
		return "", false
	}
	return name, true
}

func abrChildRenditionAdvertised(pathName string, renditions []conf.HLSTranscodingRendition) bool {
	name, ok := abrChildRenditionName(pathName)
	if !ok {
		return true
	}
	for _, r := range renditions {
		if r.Name == name {
			return true
		}
	}
	return false
}


func shouldRenderABRMaster(pathName string, pathConf *conf.Path) bool {
	return pathConf != nil && pathConf.HLSTranscoding && len(pathConf.HLSTranscodingRenditions) > 0 &&
		!isABROutputPath(pathName) && !isConfiguredABRChildPath(pathName, pathConf)
}

// isConfiguredABRChildPath checks whether pathName is a valid ABR rendition child
// path (e.g. "<uuid>/1080") that is actually enabled in pathConf's transcoding
// renditions. It is used as a fallback when FindPathConf returns an error for a
// child path that the router doesn't know about by name.
func isConfiguredABRChildPath(pathName string, pathConf *conf.Path) bool {
	if pathConf == nil || !pathConf.HLSTranscoding {
		return false
	}
	if !isABRChildPlaylistPath(pathName) {
		return false
	}
	name, ok := abrChildRenditionName(pathName)
	if !ok {
		// "original" and "main" are always valid when transcoding is on
		lastPart := pathName[strings.LastIndex(pathName, "/")+1:]
		return lastPart == "original" || lastPart == "main"
	}
	for _, r := range pathConf.HLSTranscodingRenditions {
		if r.Name == name {
			return true
		}
	}
	return false
}

func masterPlaylistRenditions(pathConf *conf.Path, mi *muxerInstance) []conf.HLSTranscodingRendition {
	if mi != nil && mi.hlsTranscodingRenditions != nil {
		return append([]conf.HLSTranscodingRendition(nil), mi.hlsTranscodingRenditions...)
	}
	if pathConf == nil {
		return nil
	}
	return append([]conf.HLSTranscodingRendition(nil), pathConf.HLSTranscodingRenditions...)
}

func renderABRMasterPlaylist(renditions []conf.HLSTranscodingRendition, codecStr string, hasAudioOpts ...bool) []byte {
	hasAudio := false
	if len(hasAudioOpts) > 0 {
		hasAudio = hasAudioOpts[0]
	}

	var b strings.Builder
	b.WriteString("#EXTM3U\n")
	b.WriteString("#EXT-X-VERSION:7\n")
	b.WriteString("#EXT-X-INDEPENDENT-SEGMENTS\n")

	if codecStr == "" {
		codecStr = defaultMasterCodecString
	}


	// Derive original bandwidth and top resolution from the highest configured rendition
	originalBandwidth := sourceBandwidth
	var topWidth, topHeight int
	for _, r := range renditions {
		bw := parseBitrate(r.VideoBitrate) + sharedAudioBandwidth
		if bw > originalBandwidth {
			originalBandwidth = bw
		}
		if r.Width > topWidth {
			topWidth = r.Width
			topHeight = r.Height
		}
	}
	originalBandwidth = originalBandwidth * 120 / 100

	resolutionAttr := ""
	if topWidth > 0 && topHeight > 0 {
		resolutionAttr = fmt.Sprintf("RESOLUTION=%dx%d,FRAME-RATE=30.000,VIDEO-RANGE=SDR,", topWidth, topHeight)
	}

	audioAttr := ""
	if hasAudio {
		audioURI := "original/audio2_stream.m3u8"
		if len(renditions) > 0 {
			audioURI = fmt.Sprintf("%s/audio2_stream.m3u8", renditions[0].Name)
		}
		b.WriteString(fmt.Sprintf(
			"#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",NAME=\"audio\",DEFAULT=YES,AUTOSELECT=YES,URI=\"%s\"\n",
			audioURI,
		))
		audioAttr = "AUDIO=\"audio\","
	}

	// 1. Original quality (LL-HLS, not uploaded to provider). When source codec
	// metadata is not available here, omit CODECS rather than advertising the
	// normalized transcoder codec.
	b.WriteString(fmt.Sprintf(
		"#EXT-X-STREAM-INF:BANDWIDTH=%d,AVERAGE-BANDWIDTH=%d,%s%sCLOSED-CAPTIONS=NONE,NAME=\"original\"\n",
		originalBandwidth, originalBandwidth, resolutionAttr, audioAttr,
	))
	b.WriteString("original/index.m3u8\n")

	// 2. Transcoded qualities from config (uploaded to provider, fMP4 segment)
	for _, r := range renditions {
		videoBandwidth := parseBitrate(r.VideoBitrate)
		bandwidth := videoBandwidth + sharedAudioBandwidth
		if bandwidth == sharedAudioBandwidth {
			bandwidth = defaultFallbackVideoBandwidth + sharedAudioBandwidth
		}
		b.WriteString(fmt.Sprintf(
			"#EXT-X-STREAM-INF:BANDWIDTH=%d,AVERAGE-BANDWIDTH=%d,RESOLUTION=%dx%d,FRAME-RATE=30.000,VIDEO-RANGE=SDR,CODECS=\"%s\",%sCLOSED-CAPTIONS=NONE,NAME=\"%sp\"\n",
			bandwidth, bandwidth, r.Width, r.Height, codecStr, audioAttr, r.Name,
		))
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

func codecStringForTranscodedOutput(pathConf *conf.Path, info *transcoder.SourceInfo) string {
	videoCodec := codecLibx264
	audioCodec := codecAAC
	profile := ""
	level := ""
	if pathConf != nil {
		videoCodec = defaultString(pathConf.HLSTranscodingVideoCodec, videoCodec)
		audioCodec = defaultString(pathConf.HLSTranscodingAudioCodec, audioCodec)
	}
	if info != nil {
		profile = info.Profile
		level = info.Level
	}
	return codecStringFromInfo(videoCodec, profile, level, audioCodec)
}

// codecStringFromInfo returns an HLS CODECS attribute for the encoded output.
// Empty/unknown input falls back to deterministic H.264 High L4.0 + AAC-LC.
func codecStringFromInfo(videoCodec, profile, level string, audioCodec string) string {
	video := defaultAVCCodecString
	videoCodec = strings.ToLower(strings.TrimSpace(videoCodec))
	if videoCodec == codecLibx264 || videoCodec == codecH264 || videoCodec == codecAVC {
		video = avcCodecString(profile, level)
	} else if videoCodec == codecHEVC || videoCodec == codecH265 || videoCodec == codecLibx265 {
		video = defaultHEVCCodecString
	}

	audio := defaultAACCodecString
	audioCodec = strings.ToLower(strings.TrimSpace(audioCodec))
	if audioCodec == codecOpus || audioCodec == codecLibopus {
		audio = defaultOpusCodecString
	}

	return video + codecSeparator + audio
}

func avcCodecString(profile, level string) string {
	profileHex := avcProfileHighHex
	switch strings.ToLower(strings.TrimSpace(profile)) {
	case avcProfileNameBaseline, avcProfileNameConstrainedBaseline:
		profileHex = avcProfileBaselineHex
	case avcProfileNameMain:
		profileHex = avcProfileMainHex
	case avcProfileNameHigh:
		profileHex = avcProfileHighHex
	case avcProfileNameHigh10:
		profileHex = avcProfileHigh10Hex
	}
	levelHex := avcDefaultLevelHex
	if strings.TrimSpace(level) != "" {
		parts := strings.SplitN(strings.TrimSpace(level), ".", 2)
		major, err := strconv.Atoi(parts[0])
		if err != nil || major <= 0 {
			return defaultAVCCodecString
		}
		minor := 0
		if len(parts) > 1 {
			minor, err = strconv.Atoi(parts[1])
			if err != nil || minor < 0 {
				return defaultAVCCodecString
			}
		}
		levelHex = fmt.Sprintf("%02x", major*10+minor)
	}
	return fmt.Sprintf(avcCodecFormat, profileHex, levelHex)
}

func defaultString(v string, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}
