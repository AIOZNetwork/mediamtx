package hls

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/bluenviron/gohlslib/v2"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/stretchr/testify/require"

	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/defs"
	"github.com/bluenviron/mediamtx/internal/stream"
	"github.com/bluenviron/mediamtx/internal/test"
	"github.com/bluenviron/mediamtx/internal/unit"
)

type namedPath struct {
	dummyPath
	name string
}

func (pa *namedPath) Name() string {
	return pa.name
}

// TestServerABRChildWithoutSession guards ABR playback. The master playlist is
// rendered without a session, so the outputs it links - renditions and the
// source ("original") - must be served without one; they answered 401 before.
// A path without transcoding keeps upstream's session requirement, even when
// its name looks like a rendition.
func TestServerABRChildWithoutSession(t *testing.T) {
	strm := &stream.Stream{
		OrigDesc: &description.Session{
			Medias: []*description.Media{test.MediaH264},
		},
		WriteQueueSize:    512,
		RTPMaxPayloadSize: 1450,
		Parent:            test.NilLogger,
	}
	err := strm.Initialize()
	require.NoError(t, err)
	defer strm.Close()

	transcoded := &conf.Path{
		HLSTranscoding: true,
		HLSTranscodingRenditions: []conf.HLSTranscodingRendition{
			{Name: "720", Width: 1280, Height: 720},
			{Name: "480", Width: 854, Height: 480},
		},
	}

	pm := &dummyPathManager{
		// always-remux creates a muxer for every ready path
		setHLSServerImpl: func() []defs.Path {
			return []defs.Path{
				&namedPath{name: "live"},
				&namedPath{name: "live/720"},
				&namedPath{name: "cam"},
				&namedPath{name: "cam/720"},
			}
		},
		findPathConfImpl: func(req defs.PathFindPathConfReq) (*defs.PathFindPathConfRes, error) {
			if req.AccessRequest.Name == "live" || req.AccessRequest.Name == "gone" {
				return &defs.PathFindPathConfRes{Conf: transcoded}, nil
			}
			return &defs.PathFindPathConfRes{Conf: &conf.Path{}}, nil
		},
		addReaderImpl: func(req defs.PathAddReaderReq) (*defs.PathAddReaderRes, error) {
			return &defs.PathAddReaderRes{
				Path:   &namedPath{name: req.AccessRequest.Name},
				Stream: strm,
			}, nil
		},
	}

	s := &Server{
		Address:         "127.0.0.1:8888",
		AlwaysRemux:     true,
		Variant:         conf.HLSVariant(gohlslib.MuxerVariantMPEGTS),
		SegmentCount:    7,
		SegmentDuration: conf.Duration(1 * time.Second),
		PartDuration:    conf.Duration(200 * time.Millisecond),
		SegmentMaxSize:  50 * 1024 * 1024,
		ReadTimeout:     conf.Duration(10 * time.Second),
		WriteTimeout:    conf.Duration(10 * time.Second),
		PathManager:     pm,
		Parent:          test.NilLogger,
	}
	err = s.Initialize()
	require.NoError(t, err)
	defer s.Close()

	hc := &http.Client{Timeout: 2 * time.Second}

	subStream := &stream.SubStream{Stream: strm}
	err = subStream.Initialize()
	require.NoError(t, err)

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for i := int64(0); ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(100 * time.Millisecond):
				subStream.WriteUnit(test.MediaH264, test.FormatH264, &unit.Unit{
					PTS:     i * 9000,
					Payload: unit.PayloadH264{{5, 1}}, // IDR
				})
			}
		}
	}()

	get := func(p string) (int, string) {
		res, err2 := hc.Get("http://127.0.0.1:8888/" + p)
		if err2 != nil {
			return 0, err2.Error()
		}
		defer res.Body.Close()
		body, _ := io.ReadAll(res.Body)
		return res.StatusCode, string(body)
	}

	// firstURI returns the first URI a playlist links.
	firstURI := func(playlist string) string {
		for line := range strings.SplitSeq(playlist, "\n") {
			if line != "" && !strings.HasPrefix(line, "#") {
				return line
			}
		}
		return ""
	}

	// Follow each ABR output the way a player does. The output is a variant
	// of the master playlist, so its index.m3u8 is a media playlist - a
	// master naming another master is invalid HLS and video.js rejects it -
	// that links segments directly; then fetch a segment.
	for _, child := range []string{"live/720", "live/original"} {
		var segmentURI string
		require.Eventually(t, func() bool {
			code, body := get(child + "/index.m3u8")
			segmentURI = firstURI(body)
			return code == http.StatusOK && segmentURI != ""
		}, 10*time.Second, 200*time.Millisecond, "%s/index.m3u8", child)

		_, body := get(child + "/index.m3u8")
		require.Contains(t, body, "#EXT-X-TARGETDURATION", child)
		require.NotContains(t, body, "#EXT-X-STREAM-INF", child)
		require.NotContains(t, body, "#EXT-X-GAP", child)

		code, _ := get(child + "/" + segmentURI)
		require.Equal(t, http.StatusOK, code, "%s/%s", child, segmentURI)
	}

	// The master playlist lists every rendition while the stream is starting,
	// then only those served here: 480 is configured but never published.
	prevWarmup := abrRenditionWarmup
	defer func() { abrRenditionWarmup = prevWarmup }()

	abrRenditionWarmup = time.Hour
	code, body := get("live/index.m3u8")
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, "720/index.m3u8")
	require.Contains(t, body, "480/index.m3u8")

	abrRenditionWarmup = 0
	code, body = get("live/index.m3u8")
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, "original/index.m3u8")
	require.Contains(t, body, "720/index.m3u8")
	require.NotContains(t, body, "480/index.m3u8")

	// live/480 is configured but not published yet: the stream is live here,
	// so players get an empty playlist and retry.
	code, body = get("live/480/index.m3u8")
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, "#EXT-X-MEDIA-SEQUENCE")

	// "gone" is not live on this server: 404, so that a proxy in front of
	// several servers asks the next one instead of serving an empty playlist.
	for _, p := range []string{"gone/720/index.m3u8", "gone/720/main_stream.m3u8", "gone/original/index.m3u8"} {
		code, _ = get(p)
		require.Equal(t, http.StatusNotFound, code, p)
	}

	// No transcoding: a session is still required, even for a name that
	// looks like a rendition.
	for _, p := range []string{
		"cam/720/stream.m3u8",
		"cam/stream.m3u8",
	} {
		code, _ = get(p)
		require.Equal(t, http.StatusUnauthorized, code, p)
	}
}
