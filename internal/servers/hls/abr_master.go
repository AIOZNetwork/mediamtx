package hls

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	codecsh264 "github.com/bluenviron/mediacommon/v2/pkg/codecs/h264"

	"github.com/bluenviron/mediamtx/internal/conf"
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
	avcConstraintNoneHex  = "00"
	avcConstraintMainHex  = "40"
	avcDefaultLevelHex    = "28"

	// H.264 Profile names
	avcProfileNameBaseline            = "baseline"
	avcProfileNameConstrainedBaseline = "constrained baseline"
	avcProfileNameMain                = "main"
	avcProfileNameHigh                = "high"
	avcProfileNameHigh10              = "high 10"

	// Codec format strings
	avcCodecFormat = "avc1.%s%s%s"
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
		if childName == "video/"+rendition.Name || childName == rendition.Name ||
			strings.TrimSuffix(childName, "p") == strings.TrimSuffix(rendition.Name, "p") {
			return true
		}
	}
	return false
}

// isABRSourceChild reports whether pathName names the source rendition of an
// ABR stream ("original", legacy "main", "video/source", "audio/main"). The
// transcoder does not publish it: the base path's own muxer serves it.
func isABRSourceChild(pathName string) bool {
	basePath := abrBasePath(pathName)
	switch strings.TrimPrefix(pathName[len(basePath):], "/") {
	case "original", "main", "video/source", "audio/main":
		return true
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
		if strings.TrimSuffix(r.Name, "p") == name {
			return true
		}
	}
	return false
}

func shouldRenderABRMaster(pathName string, pathConf *conf.Path) bool {
	return pathConf != nil && pathConf.HLSTranscoding && len(pathConf.HLSTranscodingRenditions) > 0 &&
		!isABROutputPath(pathName) && !isConfiguredABRChildPath(pathName, pathConf)
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

type abrMasterSourceInfo struct {
	codecString string
	width       int
	height      int
	fps         float64
}

func renderABRMasterPlaylist(renditions []conf.HLSTranscodingRendition, codecStr string, opts ...any) []byte {
	hasAudio := false
	sourceInfo := abrMasterSourceInfo{}
	for _, opt := range opts {
		switch v := opt.(type) {
		case bool:
			hasAudio = v
		case abrMasterSourceInfo:
			sourceInfo = v
		}
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

	originalCodecStr := codecStr
	if sourceInfo.codecString != "" {
		originalCodecStr = sourceInfo.codecString
	}
	originalFrameRate := sourceInfo.fps
	if originalFrameRate <= 0 {
		originalFrameRate = 30
	}

	resolutionAttr := ""
	if sourceInfo.width > 0 && sourceInfo.height > 0 {
		resolutionAttr = fmt.Sprintf("RESOLUTION=%dx%d,FRAME-RATE=%.3f,VIDEO-RANGE=SDR,", sourceInfo.width, sourceInfo.height, originalFrameRate)
	} else if sourceInfo == (abrMasterSourceInfo{}) && topWidth > 0 && topHeight > 0 {
		// Backward-compatible fallback for callers that cannot inspect the source track.
		resolutionAttr = fmt.Sprintf("RESOLUTION=%dx%d,FRAME-RATE=%.3f,VIDEO-RANGE=SDR,", topWidth, topHeight, originalFrameRate)
	}

	audioAttr := ""
	if hasAudio && len(renditions) == 0 {
		b.WriteString(
			"#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",NAME=\"audio\",LANGUAGE=\"und\",DEFAULT=YES,AUTOSELECT=YES,CHANNELS=\"2\",URI=\"original/audio2_stream.m3u8\"\n",
		)
		audioAttr = "AUDIO=\"audio\","
	}

	// Never mix the source and transcoded outputs: independently produced HLS
	// timelines are not guaranteed to be switch-aligned.
	if len(renditions) == 0 {
		b.WriteString(fmt.Sprintf(
			"#EXT-X-STREAM-INF:BANDWIDTH=%d,AVERAGE-BANDWIDTH=%d,%sCODECS=\"%s\",%sCLOSED-CAPTIONS=NONE\n",
			originalBandwidth, originalBandwidth, resolutionAttr, originalCodecStr, audioAttr,
		))
		b.WriteString("original/index.m3u8\n")
	}

	// 2. Transcoded qualities from config (uploaded to provider, fMP4 segment)
	for _, r := range renditions {
		videoBandwidth := parseBitrate(r.VideoBitrate)
		bandwidth := videoBandwidth + sharedAudioBandwidth
		if bandwidth == sharedAudioBandwidth {
			bandwidth = defaultFallbackVideoBandwidth + sharedAudioBandwidth
		}
		renditionFrameRate := renditionFrameRate(r)
		b.WriteString(fmt.Sprintf(
			"#EXT-X-STREAM-INF:BANDWIDTH=%d,AVERAGE-BANDWIDTH=%d,RESOLUTION=%dx%d,FRAME-RATE=%.3f,VIDEO-RANGE=SDR,CODECS=\"%s\",%sCLOSED-CAPTIONS=NONE\n",
			bandwidth, bandwidth, r.Width, r.Height, renditionFrameRate, codecStringForRendition(r, codecStr), audioAttr,
		))
		b.WriteString(r.Name)
		b.WriteString("/index.m3u8\n")
	}

	return []byte(b.String())
}

func sourceInfoFromDescription(desc *description.Session, fallbackCodecStr string) abrMasterSourceInfo {
	info := abrMasterSourceInfo{codecString: fallbackCodecStr}
	if desc == nil {
		return info
	}

	for _, media := range desc.Medias {
		if media.Type != description.MediaTypeVideo {
			continue
		}
		for _, forma := range media.Formats {
			if formaH264, ok := forma.(*format.H264); ok {
				if len(formaH264.SPS) >= 4 {
					info.codecString = "avc1." + hex.EncodeToString(formaH264.SPS[1:4]) + codecSeparator + defaultAACCodecString
					var s codecsh264.SPS
					if err := s.Unmarshal(formaH264.SPS); err == nil {
						info.width = s.Width()
						info.height = s.Height()
					}
				}
				return info
			}
		}
	}
	return info
}

func sourceInfoFromMuxerInstance(mi *muxerInstance, fallbackCodecStr string) abrMasterSourceInfo {
	if mi == nil || mi.stream == nil {
		return abrMasterSourceInfo{codecString: fallbackCodecStr}
	}
	return sourceInfoFromDescription(mi.stream.OutDescCopy(), fallbackCodecStr)
}

func codecStringForRendition(r conf.HLSTranscodingRendition, fallbackCodecStr string) string {
	profile, level := transcodedRenditionAVCProfileLevel(r)
	if fallbackCodecStr == "" {
		return codecStringFromInfo(codecLibx264, profile, level, codecAAC)
	}
	if strings.HasPrefix(fallbackCodecStr, defaultAVCCodecString[:5]) {
		parts := strings.SplitN(fallbackCodecStr, codecSeparator, 2)
		audio := defaultAACCodecString
		if len(parts) == 2 && strings.TrimSpace(parts[1]) != "" {
			audio = strings.TrimSpace(parts[1])
		}
		return avcCodecString(profile, level) + codecSeparator + audio
	}
	return fallbackCodecStr
}

func transcodedRenditionAVCProfileLevel(r conf.HLSTranscodingRendition) (string, string) {
	if r.Height >= 1080 || r.Width >= 1920 {
		return avcProfileNameHigh, "4.0"
	}
	if r.Height >= 480 || r.Width >= 854 {
		return avcProfileNameMain, "3.1"
	}
	return avcProfileNameMain, "3.0"
}

func renditionFrameRate(_ conf.HLSTranscodingRendition) float64 {
	return 30
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

func codecStringForTranscodedOutput(pathConf *conf.Path) string {
	videoCodec := codecLibx264
	audioCodec := codecAAC
	profile := ""
	level := ""
	if pathConf != nil {
		videoCodec = defaultString(pathConf.HLSTranscodingVideoCodec, videoCodec)
		audioCodec = defaultString(pathConf.HLSTranscodingAudioCodec, audioCodec)
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
	constraintHex := avcConstraintNoneHex
	switch strings.ToLower(strings.TrimSpace(profile)) {
	case avcProfileNameBaseline, avcProfileNameConstrainedBaseline:
		profileHex = avcProfileBaselineHex
	case avcProfileNameMain:
		profileHex = avcProfileMainHex
		constraintHex = avcConstraintMainHex
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
	return fmt.Sprintf(avcCodecFormat, profileHex, constraintHex, levelHex)
}

func defaultString(v string, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}
