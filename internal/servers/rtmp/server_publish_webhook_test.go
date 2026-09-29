package rtmp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluenviron/gortmplib"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/defs"
	"github.com/bluenviron/mediamtx/internal/stream"
	"github.com/bluenviron/mediamtx/internal/test"
)

// TestServerPublishWebhook guards the rule that a stream the API did not
// register never goes live: the publish webhook must answer 2xx in time, and
// while it runs the connection already shows the path and stream key the API
// reads back.
func TestServerPublishWebhook(t *testing.T) {
	const key = "22222222-2222-2222-2222-222222222222"
	media := uuid.New()

	var s *Server
	var status atomic.Int32
	var hang atomic.Bool
	var sawConn atomic.Pointer[defs.APIRTMPConn]

	webhook := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			connID, err := uuid.Parse(r.URL.Query().Get("conn_id"))
			if err == nil {
				if item, err2 := s.APIConnsGet(connID); err2 == nil {
					sawConn.Store(item)
				}
			}
			if hang.Load() {
				select {
				case <-r.Context().Done():
				case <-time.After(5 * time.Second):
				}
				return
			}
			w.WriteHeader(int(status.Load()))
		}),
	)
	defer webhook.Close()

	var published atomic.Int32
	pathManager := &test.PathManager{
		FindPathConfImpl: func(_ defs.PathFindPathConfReq) (*defs.PathFindPathConfRes, error) {
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
			published.Add(1)
			return &defs.PathAddPublisherRes{
				Path:      &dummyPath{},
				SubStream: subStream,
			}, nil
		},
	}

	s = &Server{
		Address:      "127.0.0.1:1939",
		ReadTimeout:  conf.Duration(10 * time.Second),
		WriteTimeout: conf.Duration(10 * time.Second),
		PathManager:  pathManager,
		Parent:       test.NilLogger,
		StreamKeys: &fakeStreamKeys{
			targets: map[string]*PublishTarget{
				key: {
					PathName:  media.String(),
					StreamKey: key,
					KeyType:   "video",
				},
			},
			statistics: &fakeStatistics{
				bitrate: map[uuid.UUID]float64{},
				fps:     map[uuid.UUID]int16{},
			},
		},
		PublishWebhook:        webhook.URL + "/connect?conn_type=$MTX_CONN_TYPE&conn_id=$MTX_CONN_ID",
		PublishWebhookTimeout: conf.Duration(500 * time.Millisecond),
	}
	err := s.Initialize()
	require.NoError(t, err)
	defer s.Close()

	publish := func() error {
		u, err2 := url.Parse("rtmp://127.0.0.1:1939/live/" + key)
		require.NoError(t, err2)
		c := &gortmplib.Client{URL: u, Publish: true}
		err2 = c.Initialize(context.Background())
		if err2 == nil {
			c.Close()
		}
		return err2
	}

	t.Run("2xx accepts", func(t *testing.T) {
		status.Store(http.StatusOK)
		require.NoError(t, publish())

		item := sawConn.Load()
		require.NotNil(
			t,
			item,
			"the webhook must be able to read the connection back",
		)
		require.Equal(t, media.String(), item.Path)
		require.Equal(t, key, item.StreamKey)
	})

	t.Run("5xx refuses", func(t *testing.T) {
		status.Store(http.StatusInternalServerError)
		require.ErrorContains(t, publish(), "NetStream.Publish.Unauthorized")
	})

	t.Run("timeout refuses", func(t *testing.T) {
		hang.Store(true)
		defer hang.Store(false)
		start := time.Now()
		require.ErrorContains(t, publish(), "NetStream.Publish.Unauthorized")
		require.Less(t, time.Since(start), 3*time.Second)
	})

	require.Equal(
		t,
		int32(0),
		published.Load(),
		"no refused stream may reach the path",
	)
}
