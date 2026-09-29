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
			if req.AccessRequest.Name == "live" {
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

	// Follow each ABR output the way a player does: its multivariant
	// playlist, the media playlist it links, then a segment of that.
	for _, child := range []string{"live/720", "live/original"} {
		var mediaURI string
		require.Eventually(t, func() bool {
			code, body := get(child + "/index.m3u8")
			mediaURI = firstURI(body)
			return code == http.StatusOK && strings.HasSuffix(strings.Split(mediaURI, "?")[0], ".m3u8")
		}, 10*time.Second, 200*time.Millisecond, "%s/index.m3u8", child)

		var segmentURI string
		require.Eventually(t, func() bool {
			code, body := get(child + "/" + mediaURI)
			segmentURI = firstURI(body)
			return code == http.StatusOK && segmentURI != ""
		}, 10*time.Second, 200*time.Millisecond, "%s/%s", child, mediaURI)

		code, _ := get(child + "/" + segmentURI)
		require.Equal(t, http.StatusOK, code, "%s/%s", child, segmentURI)
	}

	// No transcoding: a session is still required, even for a name that
	// looks like a rendition.
	for _, p := range []string{
		"cam/720/stream.m3u8",
		"cam/stream.m3u8",
	} {
		code, _ := get(p)
		require.Equal(t, http.StatusUnauthorized, code, p)
	}
}
