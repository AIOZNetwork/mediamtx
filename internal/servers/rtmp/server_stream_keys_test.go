package rtmp

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bluenviron/gortmplib"
	"github.com/bluenviron/gortmplib/pkg/codecs"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/bluenviron/mediamtx/internal/auth"
	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/defs"
	"github.com/bluenviron/mediamtx/internal/models"
	"github.com/bluenviron/mediamtx/internal/stream"
	"github.com/bluenviron/mediamtx/internal/test"
)

type fakeStatistics struct {
	mutex   sync.Mutex
	bitrate map[uuid.UUID]float64
	fps     map[uuid.UUID]int16
}

func (f *fakeStatistics) UpsertBitrateIn(id uuid.UUID, bitrate float64) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.bitrate[id] = bitrate
	return nil
}

func (f *fakeStatistics) UpsertFPSIn(id uuid.UUID, fps int16) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.fps[id] = fps
	return nil
}

func (f *fakeStatistics) UpsertBitrateOut(
	uuid.UUID,
	float64,
) error {
	return nil
}

func (f *fakeStatistics) UpsertFPSOut(
	uuid.UUID,
	int16,
) error {
	return nil
}

func (f *fakeStatistics) UpsertDataTransferred(
	uuid.UUID,
	float64,
) error {
	return nil
}

func (f *fakeStatistics) get(id uuid.UUID) (float64, int16, bool) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	bitrate, ok := f.bitrate[id]
	return bitrate, f.fps[id], ok
}

type fakeStreamKeys struct {
	targets    map[string]*PublishTarget
	statistics *fakeStatistics
}

func (f *fakeStreamKeys) ResolvePublish(
	_ context.Context,
	u *url.URL,
	isPublishing func(string) bool,
) (*PublishTarget, error) {
	path := strings.Trim(u.Path, "/")
	key := path[strings.LastIndex(path, "/")+1:]

	if isPublishing(key) {
		return nil, errStreamKeyPublishing
	}

	target, ok := f.targets[key]
	if !ok {
		return nil, errStreamKeyInvalid
	}
	return target, nil
}

func (f *fakeStreamKeys) Statistics() models.LiveStreamStatisticRepository {
	return f.statistics
}

// TestServerPublishStreamKey guards the AIOZ publish flow: a publish URL names
// a stream key, and the stream is published under the key's media session.
// Upstream's v1.21 rewrite of runPublish dropped it once without any test
// noticing.
func TestServerPublishStreamKey(t *testing.T) {
	prevSecondCalculate := conf.SecondCalculate
	conf.SecondCalculate = 1
	defer func() { conf.SecondCalculate = prevSecondCalculate }()

	const (
		videoKey = "22222222-2222-2222-2222-222222222222"
		audioKey = "33333333-3333-3333-3333-333333333333"
	)
	videoMedia := uuid.New()
	audioMedia := uuid.New()

	streamKeys := &fakeStreamKeys{
		targets: map[string]*PublishTarget{
			videoKey: {
				PathName:  videoMedia.String(),
				StreamKey: videoKey,
				KeyType:   "video",
			},
			audioKey: {
				PathName:  audioMedia.String(),
				StreamKey: audioKey,
				KeyType:   liveStreamKeyTypeAudio,
			},
		},
		statistics: &fakeStatistics{
			bitrate: map[uuid.UUID]float64{},
			fps:     map[uuid.UUID]int16{},
		},
	}

	var mutex sync.Mutex
	published := map[string]defs.PathAddPublisherReq{}
	var streams []*stream.Stream
	defer func() {
		for _, strm := range streams {
			strm.Close()
		}
	}()

	pathManager := &test.PathManager{
		// These run on the server's goroutines, where require would stop the
		// goroutine rather than the test: a wrong path is refused instead, and
		// the publish that expects to succeed fails.
		FindPathConfImpl: func(req defs.PathFindPathConfReq) (*defs.PathFindPathConfRes, error) {
			if _, err := uuid.Parse(req.AccessRequest.Name); err != nil {
				return nil, &auth.Error{Wrapped: fmt.Errorf("path %q is not a media id", req.AccessRequest.Name)}
			}
			return &defs.PathFindPathConfRes{}, nil
		},
		AddPublisherImpl: func(req defs.PathAddPublisherReq) (*defs.PathAddPublisherRes, error) {
			strm := &stream.Stream{
				OrigDesc:          req.Desc,
				WriteQueueSize:    512,
				RTPMaxPayloadSize: 1450,
				Parent:            test.NilLogger,
			}
			if err := strm.Initialize(); err != nil {
				return nil, err
			}

			subStream := &stream.SubStream{Stream: strm}
			if err := subStream.Initialize(); err != nil {
				return nil, err
			}

			mutex.Lock()
			published[req.AccessRequest.Name] = req
			streams = append(streams, strm)
			mutex.Unlock()

			return &defs.PathAddPublisherRes{
				Path:      &dummyPath{},
				SubStream: subStream,
			}, nil
		},
	}

	s := &Server{
		Address:      "127.0.0.1:1939",
		ReadTimeout:  conf.Duration(10 * time.Second),
		WriteTimeout: conf.Duration(10 * time.Second),
		PathManager:  pathManager,
		Parent:       test.NilLogger,
		StreamKeys:   streamKeys,
	}
	err := s.Initialize()
	require.NoError(t, err)
	defer s.Close()

	publish := func(key string) (*gortmplib.Client, *gortmplib.Writer, error) {
		u, err2 := url.Parse("rtmp://127.0.0.1:1939/live/" + key)
		require.NoError(t, err2)

		c := &gortmplib.Client{URL: u, Publish: true}
		err2 = c.Initialize(context.Background())
		if err2 != nil {
			return nil, nil, err2
		}

		w := &gortmplib.Writer{
			Conn: c,
			Tracks: []*gortmplib.Track{
				{
					Codec: &codecs.H264{
						SPS: test.FormatH264.SPS,
						PPS: test.FormatH264.PPS,
					},
				},
				{
					Codec: &codecs.MPEG4Audio{
						Config: test.FormatMPEG4Audio.Config,
					},
				},
			},
		}
		err2 = w.Initialize()
		require.NoError(t, err2)

		// The server learns the tracks from the first packets.
		err2 = w.WriteH264(w.Tracks[0], 2*time.Second, 2*time.Second, [][]byte{{5, 2, 3, 4}})
		require.NoError(t, err2)
		return c, w, nil
	}

	publishedReq := func(name string) (defs.PathAddPublisherReq, bool) {
		mutex.Lock()
		defer mutex.Unlock()
		req, ok := published[name]
		return req, ok
	}

	t.Run("video key", func(t *testing.T) {
		c, w, err2 := publish(videoKey)
		require.NoError(t, err2)
		defer c.Close()

		require.Eventually(t, func() bool {
			_, ok := publishedReq(videoMedia.String())
			return ok
		}, 5*time.Second, 50*time.Millisecond)

		req, _ := publishedReq(videoMedia.String())
		require.Equal(t, videoKey, req.StreamKey)
		require.Len(t, req.Desc.Medias, 2)

		list, err2 := s.APIConnsList()
		require.NoError(t, err2)
		require.Len(t, list.Items, 1)
		require.Equal(t, videoMedia.String(), list.Items[0].Path)
		require.Equal(t, videoKey, list.Items[0].StreamKey)

		t.Run("duplicate refused", func(t *testing.T) {
			_, _, err3 := publish(videoKey)
			require.ErrorContains(t, err3, "NetStream.Publish.Unauthorized")
		})

		t.Run("statistics", func(t *testing.T) {
			stop := make(chan struct{})
			defer close(stop)
			go func() {
				pts := 2 * time.Second
				for {
					select {
					case <-stop:
						return
					case <-time.After(40 * time.Millisecond):
						pts += 40 * time.Millisecond
						// The connection closes when the test ends; a failed
						// write then is expected.
						_ = w.WriteH264(w.Tracks[0], pts, pts, [][]byte{{5, 2, 3, 4}})
					}
				}
			}()

			require.Eventually(t, func() bool {
				bitrate, fps, ok := streamKeys.statistics.get(videoMedia)
				return ok && bitrate > 0 && fps > 0
			}, 5*time.Second, 100*time.Millisecond)
		})
	})

	t.Run("key free after disconnect", func(t *testing.T) {
		require.Eventually(t, func() bool {
			return !s.isStreamKeyPublishing(videoKey)
		}, 5*time.Second, 50*time.Millisecond)
	})

	t.Run("unknown key refused", func(t *testing.T) {
		_, _, err2 := publish(uuid.NewString())
		require.ErrorContains(t, err2, "NetStream.Publish.Unauthorized")
	})

	t.Run("audio key publishes audio only", func(t *testing.T) {
		c, w, err2 := publish(audioKey)
		require.NoError(t, err2)
		defer c.Close()

		require.Eventually(t, func() bool {
			_, ok := publishedReq(audioMedia.String())
			return ok
		}, 5*time.Second, 50*time.Millisecond)

		req, _ := publishedReq(audioMedia.String())
		require.Equal(t, audioKey, req.StreamKey)
		require.Len(t, req.Desc.Medias, 1)
		require.Equal(t, description.MediaTypeAudio, req.Desc.Medias[0].Type)

		// Video still arrives from the encoder; it must be discarded, not
		// end the connection.
		err2 = w.WriteH264(
			w.Tracks[0],
			2*time.Second,
			2*time.Second,
			[][]byte{{5, 2, 3, 4}},
		)
		require.NoError(t, err2)
		time.Sleep(200 * time.Millisecond)

		list, err2 := s.APIConnsList()
		require.NoError(t, err2)
		require.Len(t, list.Items, 1)
		require.Equal(t, defs.APIRTMPConnStatePublish, list.Items[0].State)
	})
}
