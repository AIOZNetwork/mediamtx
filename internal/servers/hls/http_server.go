package hls

import (
	_ "embed"
	"errors"
	"net"
	"net/http"
	"net/url"
	gopath "path"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/bluenviron/mediamtx/internal/auth"
	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/defs"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/protocols/httpp"
	"github.com/bluenviron/mediamtx/internal/restrictnetwork"
)

//go:generate go run ./hlsjsdownloader

//go:embed index.html
var hlsIndex []byte

//go:embed hls.min.js
var hlsMinJS []byte

var hlsPlaceholderPlaylist = []byte("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n")

func writeHLSPlaceholderPlaylist(ctx *gin.Context) {
	ctx.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	ctx.Header("Content-Type", "application/vnd.apple.mpegurl")
	ctx.Writer.WriteHeader(http.StatusOK)
	ctx.Writer.Write(hlsPlaceholderPlaylist)
}

func mergePathAndQuery(path string, rawQuery string) string {
	res := path
	if rawQuery != "" {
		res += "?" + rawQuery
	}
	return res
}

type httpServer struct {
	address        string
	encryption     bool
	serverKey      string
	serverCert     string
	allowOrigin    string
	trustedProxies conf.IPNetworks
	readTimeout    conf.Duration
	pathManager    serverPathManager
	parent         *Server

	inner *httpp.Server
}

func (s *httpServer) initialize() error {
	router := gin.New()
	router.SetTrustedProxies(s.trustedProxies.ToTrustedProxies()) //nolint:errcheck

	router.GET("/ping", func(ctx *gin.Context) {
		ctx.String(http.StatusOK, "pong")
	})

	router.Use(s.middlewareOrigin)

	router.Use(s.onRequest)

	network, address := restrictnetwork.Restrict("tcp", s.address)

	s.inner = &httpp.Server{
		Network:     network,
		Address:     address,
		ReadTimeout: time.Duration(s.readTimeout),
		Encryption:  s.encryption,
		ServerCert:  s.serverCert,
		ServerKey:   s.serverKey,
		Handler:     router,
		Parent:      s,
	}
	err := s.inner.Initialize()
	if err != nil {
		return err
	}

	return nil
}

// Log implements logger.Writer.
func (s *httpServer) Log(level logger.Level, format string, args ...interface{}) {
	s.parent.Log(level, format, args...)
}

func (s *httpServer) close() {
	s.inner.Close()
}

func (s *httpServer) middlewareOrigin(ctx *gin.Context) {
	origin := ctx.Request.Header.Get("Origin")
	if origin != "" && (s.allowOrigin == "*" || s.allowOrigin == origin) {
		ctx.Header("Access-Control-Allow-Origin", origin)
		ctx.Header("Vary", "Origin")
	} else {
		ctx.Header("Access-Control-Allow-Origin", s.allowOrigin)
	}
	ctx.Header("Access-Control-Allow-Credentials", "true")

	// preflight requests
	if ctx.Request.Method == http.MethodOptions &&
		ctx.Request.Header.Get("Access-Control-Request-Method") != "" {
		ctx.Header("Access-Control-Allow-Methods", "OPTIONS, GET")
		ctx.Header("Access-Control-Allow-Headers", "Authorization, Range")
		ctx.AbortWithStatus(http.StatusNoContent)
		return
	}
}

func (s *httpServer) onRequest(ctx *gin.Context) {
	if ctx.Request.Method != http.MethodGet {
		return
	}

	// remove leading prefix
	pa := ctx.Request.URL.Path[1:]
	if strings.HasPrefix(pa, "media/") {
		s.onMediaRequest(ctx, strings.TrimPrefix(pa, "media/"))
		return
	}

	var dir string
	var fname string

	switch {
	case strings.HasSuffix(pa, "/hls.min.js"):
		ctx.Header("Cache-Control", "max-age=3600")
		ctx.Header("Content-Type", "application/javascript")
		ctx.Writer.WriteHeader(http.StatusOK)
		ctx.Writer.Write(hlsMinJS)
		return

	case pa == "", pa == "favicon.ico", strings.HasSuffix(pa, "/hls.min.js.map"):
		return

	case strings.HasSuffix(pa, ".m3u8") ||
		strings.HasSuffix(pa, ".ts") ||
		strings.HasSuffix(pa, ".mp4") ||
		strings.HasSuffix(pa, ".m4s") ||
		strings.HasSuffix(pa, ".mp"):
		dir, fname = gopath.Dir(pa), gopath.Base(pa)

		if strings.HasSuffix(fname, ".mp") {
			fname += "4"
		}

	default:
		dir, fname = pa, ""

		if !strings.HasSuffix(dir, "/") {
			ctx.Header("Location", mergePathAndQuery(ctx.Request.URL.Path+"/", ctx.Request.URL.RawQuery))
			ctx.Writer.WriteHeader(http.StatusMovedPermanently)
			return
		}
	}

	dir = strings.TrimSuffix(dir, "/")
	if dir == "" {
		return
	}

	pathConfName := dir
	abrChild := false
	if isABRChildPlaylistPath(dir) {
		pathConfName = abrBasePath(dir)
		abrChild = true
	}

	req := defs.PathAccessRequest{
		Name:    pathConfName,
		Publish: false,
		IP:      net.ParseIP(ctx.ClientIP()),
		Proto:   auth.ProtocolHLS,
	}
	req.FillFromHTTPRequest(ctx.Request)

	pathConf, err := s.pathManager.FindPathConf(defs.PathFindPathConfReq{
		AccessRequest: req,
	})
	if err != nil && !abrChild && strings.Contains(dir, "/") {
		basePath := abrBasePath(dir)
		req.Name = basePath
		candidatePathConf, candidateErr := s.pathManager.FindPathConf(defs.PathFindPathConfReq{
			AccessRequest: req,
		})
		if candidateErr == nil && isConfiguredABRChildPath(dir, candidatePathConf) {
			pathConfName = basePath
			pathConf = candidatePathConf
			err = nil
			abrChild = true
		}
	}
	if err != nil {
		var terr auth.Error
		if errors.As(err, &terr) {
			if terr.AskCredentials {
				ctx.Header("WWW-Authenticate", `Basic realm="mediamtx"`)
				ctx.Writer.WriteHeader(http.StatusUnauthorized)
				return
			}

			s.Log(logger.Info, "connection %v failed to authenticate: %v", httpp.RemoteAddr(ctx), terr.Message)

			// wait some seconds to mitigate brute force attacks
			<-time.After(auth.PauseAfterError)

			ctx.Writer.WriteHeader(http.StatusUnauthorized)
			return
		}

		ctx.Writer.WriteHeader(http.StatusNotFound)
		return
	}

	switch fname {
	case "":
		ctx.Header("Cache-Control", "max-age=3600")
		ctx.Header("Content-Type", "text/html")
		ctx.Writer.WriteHeader(http.StatusOK)
		ctx.Writer.Write(hlsIndex)

	default:
		if fname == "index.m3u8" && shouldRenderABRMaster(dir, pathConf) {
			mux, err := s.parent.getMuxer(serverGetMuxerReq{
				path:           dir,
				remoteAddr:     httpp.RemoteAddr(ctx),
				query:          ctx.Request.URL.RawQuery,
				sourceOnDemand: pathConf.SourceOnDemand,
			})
			if err != nil || mux == nil {
				ctx.Writer.WriteHeader(http.StatusNotFound)
				return
			}
			mi := mux.getInstance()
			if mi == nil {
				ctx.Writer.WriteHeader(http.StatusNotFound)
				return
			}

			ctx.Header("Cache-Control", "no-cache")
			ctx.Header("Content-Type", "application/vnd.apple.mpegurl")
			ctx.Writer.WriteHeader(http.StatusOK)
			ctx.Writer.Write(renderABRMasterPlaylist(
				masterPlaylistRenditions(pathConf, mi),
				codecStringForTranscodedOutput(pathConf, nil),
				mi.hasAudio()))
			return
		}

		if fname == "index.m3u8" && s.parent.DVREnabled && s.parent.DVRService != nil {
			playlist, ok, err := s.parent.DVRService.RenderPlaylist(dir, time.Now())
			if err != nil {
				s.Log(logger.Warn, "DVR playlist error for %s: %v", dir, err)
				ctx.Writer.WriteHeader(http.StatusInternalServerError)
				return
			}
			if ok {
				ctx.Header("Cache-Control", "no-cache")
				ctx.Header("Content-Type", "application/vnd.apple.mpegurl")
				ctx.Writer.WriteHeader(http.StatusOK)
				ctx.Writer.Write(playlist)
				return
			}
		}

		muxPath := dir
		if isOriginalPath(dir) {
			muxPath = abrBasePath(dir)
		}

		var mi *muxerInstance
		isABRChildPlaylist := isABRChildPlaylistPath(dir) && !isOriginalPath(dir) &&
			(fname == "index.m3u8" || strings.HasSuffix(fname, ".m3u8"))
		if isABRChildPlaylist {
			var baseMI *muxerInstance
			baseMux, baseErr := s.parent.getMuxer(serverGetMuxerReq{
				path:           abrBasePath(dir),
				remoteAddr:     httpp.RemoteAddr(ctx),
				query:          ctx.Request.URL.RawQuery,
				sourceOnDemand: pathConf.SourceOnDemand,
			})
			if baseErr == nil && baseMux != nil {
				baseMI = baseMux.getInstance()
			}
			if !abrChildRenditionAdvertised(dir, masterPlaylistRenditions(pathConf, baseMI)) {
				ctx.Writer.WriteHeader(http.StatusNotFound)
				return
			}
		}
		mux, err := s.parent.getMuxer(serverGetMuxerReq{
			path:           muxPath,
			remoteAddr:     httpp.RemoteAddr(ctx),
			query:          ctx.Request.URL.RawQuery,
			sourceOnDemand: pathConf.SourceOnDemand,
			abrChild:       isABRChildPlaylistPath(dir) && !isOriginalPath(dir),
		})
		if err == nil && mux != nil {
			mi = mux.getInstance()
		}

		if mi == nil {
			if isABRChildPlaylist {
				writeHLSPlaceholderPlaylist(ctx)
				return
			}
			ctx.Writer.WriteHeader(http.StatusNotFound)
			return
		}

		if isABRChildPlaylist && !mi.isMediaPlaylistReady() {
			writeHLSPlaceholderPlaylist(ctx)
			return
		}

		if isABRChildPlaylistPath(dir) && fname == "index.m3u8" {
			fname = mi.primaryVideoPlaylist()
		}

		ctx.Request.URL.Path = fname
		mi.handleRequest(ctx)
	}
}

func (s *httpServer) onMediaRequest(ctx *gin.Context, mediaPath string) {
	idx := strings.LastIndex(mediaPath, "/")
	if idx <= 0 || idx == len(mediaPath)-1 {
		ctx.Writer.WriteHeader(http.StatusNotFound)
		return
	}
	streamID, err := url.PathUnescape(mediaPath[:idx])
	if err != nil {
		ctx.Writer.WriteHeader(http.StatusBadRequest)
		return
	}
	segmentName, err := url.PathUnescape(mediaPath[idx+1:])
	if err != nil {
		ctx.Writer.WriteHeader(http.StatusBadRequest)
		return
	}

	pathConfName := streamID
	abrChild := false
	if isABRChildPlaylistPath(streamID) {
		pathConfName = abrBasePath(streamID)
		abrChild = true
	}

	req := defs.PathAccessRequest{
		Name:    pathConfName,
		Publish: false,
		IP:      net.ParseIP(ctx.ClientIP()),
		Proto:   auth.ProtocolHLS,
	}
	req.FillFromHTTPRequest(ctx.Request)
	_, err = s.pathManager.FindPathConf(defs.PathFindPathConfReq{AccessRequest: req})
	if err != nil && !abrChild && strings.Contains(streamID, "/") {
		basePath := abrBasePath(streamID)
		req.Name = basePath
		candidatePathConf, candidateErr := s.pathManager.FindPathConf(defs.PathFindPathConfReq{AccessRequest: req})
		if candidateErr == nil && isConfiguredABRChildPath(streamID, candidatePathConf) {
			err = nil
			abrChild = true
		}
	}
	if err != nil {
		ctx.Writer.WriteHeader(http.StatusNotFound)
		return
	}

	if !s.parent.DVREnabled || s.parent.DVRService == nil || !s.parent.DVRService.ServeMedia(ctx.Writer, ctx.Request, streamID, segmentName) {
		ctx.Writer.WriteHeader(http.StatusNotFound)
	}
}
