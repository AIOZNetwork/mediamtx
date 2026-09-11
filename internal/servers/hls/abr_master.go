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

	// HLS Master Playlist Audio Tag attributes
	audioGroupID   = "main-audio"
	audioTrackURI  = "audio/main/index.m3u8"
	audioTrackName = "Main"

	// Path markers
	pathMarkerVideo     = "/video/"
	pathMarkerAudio     = "/audio/"
	pathMarkerAudioMain = "/audio/main"
)

func isABROutputPath(pathName string) bool {
	return strings.Contains(pathName, pathMarkerVideo) || strings.Contains(pathName, pathMarkerAudio)
}

func isABRChildPlaylistPath(pathName string) bool {
	return strings.Contains(pathName, pathMarkerVideo) || strings.Contains(pathName, pathMarkerAudioMain)
}

func abrBasePath(pathName string) string {
	for _, marker := range []string{pathMarkerVideo, pathMarkerAudio} {
		if idx := strings.Index(pathName, marker); idx >= 0 {
			return pathName[:idx]
		}
	}
	return pathName
}

func shouldRenderABRMaster(pathName string, pathConf *conf.Path) bool {
	return pathConf != nil && pathConf.HLSTranscoding && len(pathConf.HLSTranscodingRenditions) > 0 && !isABROutputPath(pathName)
}

func renderABRMasterPlaylist(renditions []conf.HLSTranscodingRendition, codecStr string) []byte {
	var b strings.Builder
	b.WriteString("#EXTM3U\n")
	b.WriteString("#EXT-X-VERSION:7\n")
	b.WriteString("#EXT-X-INDEPENDENT-SEGMENTS\n")
	b.WriteString(fmt.Sprintf(`#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="%s",NAME="%s",LANGUAGE="und",DEFAULT=YES,AUTOSELECT=YES,CHANNELS="2",URI="%s"`,
		audioGroupID, audioTrackName, audioTrackURI))
	b.WriteByte('\n')

	if codecStr == "" {
		codecStr = defaultMasterCodecString
	}

	for _, r := range renditions {
		videoBandwidth := parseBitrate(r.VideoBitrate)
		bandwidth := videoBandwidth + sharedAudioBandwidth
		if bandwidth == sharedAudioBandwidth {
			bandwidth = defaultFallbackVideoBandwidth + sharedAudioBandwidth
		}
		b.WriteString(fmt.Sprintf(
			"#EXT-X-STREAM-INF:BANDWIDTH=%d,AVERAGE-BANDWIDTH=%d,RESOLUTION=%dx%d,FRAME-RATE=30.000,VIDEO-RANGE=SDR,CODECS=\"%s\",AUDIO=\"%s\",CLOSED-CAPTIONS=NONE\n",
			bandwidth, bandwidth, r.Width, r.Height, codecStr, audioGroupID,
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
