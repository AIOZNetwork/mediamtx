package rtmp

import (
	"errors"
	"time"

	"github.com/bluenviron/gortmplib"
	"github.com/bluenviron/gortmplib/pkg/codecs"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
)

// liveStreamKeyTypeAudio is the live_stream_keys.type of a stream key that
// publishes audio only.
const liveStreamKeyTypeAudio = "audio"

var errAudioKeyNoAudio = errors.New(
	"an audio stream key needs an audio track, and the stream has none",
)

// dropVideo removes the video medias rtmp.ToStream mapped, and discards the
// data of their tracks.
//
// The video callbacks are replaced, not removed: gortmplib calls a track's
// callback for every packet without checking it is set, so a track with no
// callback would stop the connection at the first video packet.
func dropVideo(
	r *gortmplib.Reader,
	medias []*description.Media,
) ([]*description.Media, error) {
	for _, track := range r.Tracks() {
		switch track.Codec.(type) {
		case *codecs.AV1:
			r.OnDataAV1(track, func(time.Duration, [][]byte) {})
		case *codecs.VP9:
			r.OnDataVP9(track, func(time.Duration, []byte) {})
		case *codecs.H265:
			r.OnDataH265(track, func(time.Duration, time.Duration, [][]byte) {})
		case *codecs.H264:
			r.OnDataH264(track, func(time.Duration, time.Duration, [][]byte) {})
		}
	}

	var audio []*description.Media
	for _, medi := range medias {
		if medi.Type != description.MediaTypeVideo {
			audio = append(audio, medi)
		}
	}

	if len(audio) == 0 {
		return nil, errAudioKeyNoAudio
	}

	return audio, nil
}
