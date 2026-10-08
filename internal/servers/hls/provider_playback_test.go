package hls

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bluenviron/mediamtx/internal/conf"
	dbrepo "github.com/bluenviron/mediamtx/internal/database/repository"
	"github.com/bluenviron/mediamtx/internal/defs"
	"github.com/bluenviron/mediamtx/internal/models"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type providerPlaybackRepo struct {
	items []models.ProviderHLSMetadata
	calls []models.ProviderHLSMetadataWindowQuery
}

func (r *providerPlaybackRepo) UpsertByIdempotencyKey(metadata models.ProviderHLSMetadata) (*models.ProviderHLSMetadata, error) {
	return &metadata, nil
}

func (r *providerPlaybackRepo) ListWindow(query models.ProviderHLSMetadataWindowQuery) ([]models.ProviderHLSMetadata, error) {
	r.calls = append(r.calls, query)
	out := make([]models.ProviderHLSMetadata, 0)
	for _, item := range r.items {
		if item.StreamID != query.StreamID || (query.Generation > 0 && item.Generation != query.Generation) {
			continue
		}
		if query.RenditionName != "" && item.RenditionName != query.RenditionName {
			continue
		}
		out = append(out, item)
	}
	return out, nil
}

func providerPlaybackFixtures() []models.ProviderHLSMetadata {
	seq1 := int64(10)
	seq2 := int64(11)
	pdt := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	base := models.ProviderHLSMetadata{
		StreamID:      "camera/one",
		Generation:    3,
		RenditionName: "main",
		Width:         1280,
		Height:        720,
		Bandwidth:     2500000,
		Codecs:        "avc1.64001f,mp4a.40.2",
		Provider:      "depin",
		ContentType:   "video/mp4",
	}
	init := base
	init.Kind = models.ProviderHLSObjectKindInit
	init.ObjectName = "init.mp4"
	init.StorageKey = "streams/camera-one/init.mp4"
	init.URL = "https://provider.example/init.mp4"
	seg1 := base
	seg1.Kind = models.ProviderHLSObjectKindSegment
	seg1.ObjectName = "seg-10.m4s"
	seg1.StorageKey = "streams/camera-one/seg-10.m4s"
	seg1.URL = "https://provider.example/seg-10.m4s"
	seg1.MediaSequence = &seq1
	seg1.DurationMS = 2000
	seg1.ProgramDateTime = &pdt
	seg1.InitSegmentName = init.ObjectName
	seg1.InitStorageKey = init.StorageKey
	seg2 := base
	seg2.Kind = models.ProviderHLSObjectKindSegment
	seg2.ObjectName = "seg-11.m4s"
	seg2.StorageKey = "streams/camera-one/seg-11.m4s"
	seg2.URL = "https://provider.example/seg-11.m4s"
	seg2.MediaSequence = &seq2
	seg2.DurationMS = 4000
	seg2.InitSegmentName = init.ObjectName
	seg2.InitStorageKey = init.StorageKey
	return []models.ProviderHLSMetadata{seg2, init, seg1}
}

func hybridProviderFixtures() []models.ProviderHLSMetadata {
	items := providerPlaybackFixtures()
	out := make([]models.ProviderHLSMetadata, 0, len(items)*3)
	for _, rendition := range []struct {
		name      string
		width     int
		height    int
		bandwidth int64
	}{
		{"1080p", 1920, 1080, 5000000},
		{"720p", 1280, 720, 2500000},
		{"480p", 854, 480, 1000000},
	} {
		for _, item := range items {
			item.StreamID = "camera-one"
			item.RenditionName = rendition.name
			item.Width = rendition.width
			item.Height = rendition.height
			item.Bandwidth = rendition.bandwidth
			out = append(out, item)
		}
	}
	return out
}

func newHybridHTTPServer(repo *providerPlaybackRepo) *httpServer {
	return &httpServer{
		providerPlayback: &ProviderPlaybackService{Repository: repo},
		pathManager: &dummyPathManager{findPathConfImpl: func(_ defs.PathFindPathConfReq) (*defs.PathFindPathConfRes, error) {
			return &defs.PathFindPathConfRes{Conf: &conf.Path{HLSPlaybackMode: "hybrid", SourceOnDemand: true}}, nil
		}},
	}
}

func newMuxerRequestRecorder(t *testing.T) (*Server, chan serverGetMuxerReq) {
	reqs := make(chan serverGetMuxerReq, 1)
	server := &Server{
		ctx:        context.Background(),
		chGetMuxer: make(chan serverGetMuxerReq),
	}
	go func() {
		req := <-server.chGetMuxer
		reqs <- req
		req.res <- serverGetMuxerRes{err: fmt.Errorf("muxer not ready")}
	}()
	t.Cleanup(func() { close(reqs) })
	return server, reqs
}

func TestProviderPlaybackMasterPlaylistFromMetadata(t *testing.T) {
	repo := &providerPlaybackRepo{items: providerPlaybackFixtures()}
	service := &ProviderPlaybackService{Repository: repo}
	pl, err := service.RenderMasterPlaylist(ProviderPlaylistRequest{StreamID: "camera/one", Generation: 3})
	require.NoError(t, err)
	body := string(pl)
	require.Contains(t, body, "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-INDEPENDENT-SEGMENTS\n")
	require.Contains(t, body, "#EXT-X-STREAM-INF:BANDWIDTH=2500000,AVERAGE-BANDWIDTH=2500000,RESOLUTION=1280x720,CODECS=\"avc1.64001f,mp4a.40.2\",CLOSED-CAPTIONS=NONE")
	require.Contains(t, body, "main/index.m3u8?generation=3")
	require.Len(t, repo.calls, 1)
	require.Empty(t, repo.calls[0].RenditionName)
}

func TestProviderPlaybackMediaPlaylistIncludesMapAndSegments(t *testing.T) {
	repo := &providerPlaybackRepo{items: providerPlaybackFixtures()}
	service := &ProviderPlaybackService{Repository: repo}
	pl, err := service.RenderMediaPlaylist(ProviderPlaylistRequest{StreamID: "camera/one", RenditionName: "main", Generation: 3})
	require.NoError(t, err)
	body := string(pl)
	require.Contains(t, body, "#EXT-X-TARGETDURATION:4\n")
	require.Contains(t, body, "#EXT-X-MEDIA-SEQUENCE:10\n")
	require.Contains(t, body, "#EXT-X-MAP:URI=\"/provider-hls/media/camera%2Fone/main/3/init.mp4\"")
	require.Contains(t, body, "#EXT-X-PROGRAM-DATE-TIME:2026-10-02T12:00:00.000Z\n#EXTINF:2.000,\n/provider-hls/media/camera%2Fone/main/3/seg-10.m4s")
	require.True(t, strings.Index(body, "seg-10.m4s") < strings.Index(body, "seg-11.m4s"))
	require.NotContains(t, body, "#EXT-X-PART")
}

func TestProviderPlaybackObjectRouteRedirectsToProviderURL(t *testing.T) {
	repo := &providerPlaybackRepo{items: providerPlaybackFixtures()}
	service := &ProviderPlaybackService{Repository: repo}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/provider-hls/media/camera%2Fone/main/3/seg-10.m4s", nil)
	err := service.RedirectObject(rec, req, ProviderObjectRequest{
		StreamID: "camera/one", RenditionName: "main", Generation: 3, ObjectName: "seg-10.m4s",
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusFound, rec.Code)
	require.Equal(t, "https://provider.example/seg-10.m4s", rec.Header().Get("Location"))
}

func TestProviderPlaybackHasNoLocalFallback(t *testing.T) {
	repo := &providerPlaybackRepo{}
	service := &ProviderPlaybackService{Repository: repo}
	_, err := service.RenderMediaPlaylist(ProviderPlaylistRequest{StreamID: "camera/one", RenditionName: "main", Generation: 3})
	require.Error(t, err)
	require.Len(t, repo.calls, 1)
	// The provider service has only a metadata repository dependency; an empty repo fails instead of invoking muxer/filesystem fallback.
}

func TestProviderPlaybackHTTPRoutesBypassPathManager(t *testing.T) {
	items := providerPlaybackFixtures()
	for i := range items {
		items[i].StreamID = "camera-one"
	}
	repo := &providerPlaybackRepo{items: items}
	server := &httpServer{providerPlayback: &ProviderPlaybackService{Repository: repo}}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/camera-one/index.m3u8?generation=3", nil)
	server.onRequest(newGinContext(rec, req))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "main/index.m3u8?generation=3")

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/camera-one/main/index.m3u8?generation=3", nil)
	server.onRequest(newGinContext(rec, req))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "#EXT-X-MAP")

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/provider-hls/media/camera-one/main/3/seg-10.m4s", nil)
	server.onRequest(newGinContext(rec, req))
	require.Equal(t, http.StatusFound, rec.Code)
	require.Equal(t, "https://provider.example/seg-10.m4s", rec.Header().Get("Location"))
}

func TestHybridPlaybackMasterContainsOnlyProviderRenditions(t *testing.T) {
	repo := &providerPlaybackRepo{items: hybridProviderFixtures()}
	server := newHybridHTTPServer(repo)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/camera-one/index.m3u8?generation=3", nil)
	server.onRequest(newGinContext(rec, req))
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	require.NotContains(t, body, "original/index.m3u8")
	require.Contains(t, body, "1080p/index.m3u8?generation=3")
	require.Contains(t, body, "720p/index.m3u8?generation=3")
	require.Contains(t, body, "480p/index.m3u8?generation=3")
}

func TestProviderPlaybackLatestMasterAndNestedPath(t *testing.T) {
	items := providerPlaybackFixtures()
	for i := range items {
		items[i].Generation = 4
	}
	older := providerPlaybackFixtures()
	items = append(items, older...)
	repo := &providerPlaybackRepo{items: items}
	server := &httpServer{providerPlayback: &ProviderPlaybackService{Repository: repo}}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/camera/one/index.m3u8", nil)
	server.onRequest(newGinContext(rec, req))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "main/index.m3u8?generation=4")
	require.Equal(t, "camera/one", repo.calls[0].StreamID)
}

func TestProviderPlaybackCrossGenerationWindow(t *testing.T) {
	items := providerPlaybackFixtures()
	for i := range items {
		items[i].Generation = 3
	}
	newItems := providerPlaybackFixtures()
	for i := range newItems {
		newItems[i].Generation = 4
		newItems[i].ObjectName = strings.ReplaceAll(newItems[i].ObjectName, "init.mp4", "init-4.mp4")
		newItems[i].ObjectName = strings.ReplaceAll(newItems[i].ObjectName, "seg-10", "seg-12")
		newItems[i].ObjectName = strings.ReplaceAll(newItems[i].ObjectName, "seg-11", "seg-13")
		newItems[i].StorageKey += "-4"
		if newItems[i].Kind == models.ProviderHLSObjectKindSegment {
			seq := *newItems[i].MediaSequence + 2
			newItems[i].MediaSequence = &seq
			newItems[i].InitSegmentName = "init-4.mp4"
			newItems[i].InitStorageKey = "streams/camera-one/init.mp4-4"
		}
	}
	service := &ProviderPlaybackService{Repository: &providerPlaybackRepo{items: append(items, newItems...)}}
	pl, err := service.RenderMediaPlaylist(ProviderPlaylistRequest{
		StreamID: "camera/one", RenditionName: "main", Generation: 3,
	})
	require.NoError(t, err)
	body := string(pl)
	require.Contains(t, body, "#EXT-X-MEDIA-SEQUENCE:10")
	require.Contains(t, body, "#EXT-X-DISCONTINUITY-SEQUENCE:0")
	require.Equal(t, 1, strings.Count(body, "#EXT-X-DISCONTINUITY\n"))
	require.Contains(t, body, "/main/3/init.mp4")
	require.Contains(t, body, "/main/4/init-4.mp4")
	require.True(t, strings.Index(body, "seg-11.m4s") < strings.Index(body, "seg-12.m4s"))
}

func TestProviderPlaybackMalformedGeneration(t *testing.T) {
	repo := &providerPlaybackRepo{items: providerPlaybackFixtures()}
	server := &httpServer{providerPlayback: &ProviderPlaybackService{Repository: repo}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/camera/one/index.m3u8?generation=bad", nil)
	server.onRequest(newGinContext(rec, req))
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Empty(t, repo.calls)
}

func TestHybridPlaybackMasterCreatesBaseMuxer(t *testing.T) {
	repo := &providerPlaybackRepo{items: hybridProviderFixtures()}
	server := newHybridHTTPServer(repo)
	parent, reqs := newMuxerRequestRecorder(t)
	server.parent = parent

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/camera-one/index.m3u8?generation=3", nil)
	server.onRequest(newGinContext(rec, req))

	muxerReq := <-reqs
	require.Equal(t, "camera-one", muxerReq.path)
	require.True(t, muxerReq.create)
	require.Equal(t, "generation=3", muxerReq.query)
	require.True(t, muxerReq.sourceOnDemand)
}

func TestHybridPlaybackOriginalChildDoesNotUseProvider(t *testing.T) {
	repo := &providerPlaybackRepo{items: hybridProviderFixtures()}
	server := newHybridHTTPServer(repo)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/camera-one/original/index.m3u8?generation=3", nil)
	server.onRequest(newGinContext(rec, req))
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Empty(t, repo.calls)
}

func TestHybridPlaybackOriginalChildMediaObjectDoesNotUseProviderOrABRFallback(t *testing.T) {
	repo := &providerPlaybackRepo{items: hybridProviderFixtures()}
	server := newHybridHTTPServer(repo)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/camera-one/original/seg-1.m4s?generation=3", nil)
	server.onRequest(newGinContext(rec, req))
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Contains(t, rec.Body.String(), "muxer not found")
	require.Empty(t, repo.calls)
}

func TestHybridPlaybackOriginalChildCreatesBaseMuxer(t *testing.T) {
	repo := &providerPlaybackRepo{items: hybridProviderFixtures()}
	server := newHybridHTTPServer(repo)
	parent, reqs := newMuxerRequestRecorder(t)
	server.parent = parent

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/camera-one/original/seg-1.m4s?generation=3", nil)
	server.onRequest(newGinContext(rec, req))

	muxerReq := <-reqs
	require.Equal(t, "camera-one", muxerReq.path)
	require.True(t, muxerReq.create)
	require.Equal(t, "generation=3", muxerReq.query)
	require.True(t, muxerReq.sourceOnDemand)
	require.Empty(t, repo.calls)
}

func TestHybridPlaybackProviderChildUsesMetadata(t *testing.T) {
	repo := &providerPlaybackRepo{items: hybridProviderFixtures()}
	server := newHybridHTTPServer(repo)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/camera-one/720p/index.m3u8?generation=3", nil)
	server.onRequest(newGinContext(rec, req))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "#EXT-X-MAP")
	require.Len(t, repo.calls, 2)
	require.Equal(t, "camera-one/720p", repo.calls[0].StreamID)
	require.Equal(t, "720p", repo.calls[1].RenditionName)
}

func TestHybridPlaybackProviderChildRequiresGenerationNoFallback(t *testing.T) {
	repo := &providerPlaybackRepo{items: hybridProviderFixtures()}
	server := newHybridHTTPServer(repo)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/camera-one/720p/index.m3u8", nil)
	server.onRequest(newGinContext(rec, req))
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Len(t, repo.calls, 1)
	require.Equal(t, "camera-one/720p", repo.calls[0].StreamID)
}

func TestProviderPlaybackInternalE2EFromPersistedMetadata(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.ProviderHLSMetadata{}))
	repo := dbrepo.NewProviderHLSMetadataRepository(db)

	items := providerPlaybackFixtures()
	for i := range items {
		items[i].StreamID = "camera-one"
		items[i].MediaMTXPath = "camera-one"
		items[i].Generation = 7
		items[i].IdempotencyKey = items[i].Kind + ":" + items[i].ObjectName
		items[i].EventID = "event-" + items[i].ObjectName
		items[i].ContractID = "contract-a"
		items[i].PublishedAt = time.Unix(int64(100+i), 0).UTC()
	}

	// MediaMTX rejects segment metadata before the init object exists, so the
	// internal E2E path must persist/publish objects in returned staging order.
	_, err = repo.UpsertByIdempotencyKey(items[0])
	require.Error(t, err)
	_, err = repo.UpsertByIdempotencyKey(items[1])
	require.NoError(t, err)
	_, err = repo.UpsertByIdempotencyKey(items[2])
	require.NoError(t, err)
	_, err = repo.UpsertByIdempotencyKey(items[0])
	require.NoError(t, err)

	server := &httpServer{providerPlayback: &ProviderPlaybackService{Repository: repo}}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/camera-one/index.m3u8?generation=7", nil)
	server.onRequest(newGinContext(rec, req))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "main/index.m3u8?generation=7")

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/camera-one/main/index.m3u8?generation=7", nil)
	server.onRequest(newGinContext(rec, req))
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	require.Contains(t, body, "#EXT-X-MAP:URI=\"/provider-hls/media/camera-one/main/7/init.mp4\"")
	require.True(t, strings.Index(body, "seg-10.m4s") < strings.Index(body, "seg-11.m4s"))

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/provider-hls/media/camera-one/main/7/seg-10.m4s", nil)
	server.onRequest(newGinContext(rec, req))
	require.Equal(t, http.StatusFound, rec.Code)
	require.Equal(t, "https://provider.example/seg-10.m4s", rec.Header().Get("Location"))
}

func newGinContext(rec *httptest.ResponseRecorder, req *http.Request) *gin.Context {
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = req
	return ctx
}
