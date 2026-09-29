package hls

import (
	_ "embed"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/bluenviron/mediamtx/internal/auth"
	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/defs"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/protocols/httpp"
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

func trailingSlashLocation(rawPath string, rawQuery string) string {
	res := path.Clean(rawPath)
	res = strings.TrimLeft(res, "/\\")
	res = "/" + res + "/"

	if rawQuery != "" {
		res += "?" + rawQuery
	}

	return res
}

func sanitizeLocation(rawPath string, rawQuery string) string {
	res := path.Clean(rawPath)
	res = strings.TrimLeft(res, "/\\")
	res = "/" + res

	if rawQuery != "" {
		res += "?" + rawQuery
	}

	return res
}

type httpServer struct {
	address        string
	dumpPackets    bool
	encryption     bool
	serverKey      string
	serverCert     string
	allowOrigins   []string
	trustedProxies conf.IPNetworks
	readTimeout    conf.Duration
	writeTimeout   conf.Duration
	cdnSecret      string
	pathManager    serverPathManager
	parent         *Server

	inner *httpp.Server
}

func (s *httpServer) initialize() error {
	router := gin.New()
	router.SetTrustedProxies(s.trustedProxies.ToTrustedProxies()) //nolint:errcheck
	router.Use(s.middlewarePreflightRequests)
	router.Use(s.onRequest)

	var proto string
	if s.encryption {
		proto = "hlss"
	} else {
		proto = "hls"
	}

	s.inner = &httpp.Server{
		Address:           s.address,
		AllowOrigins:      s.allowOrigins,
		DumpPackets:       s.dumpPackets,
		DumpPacketsPrefix: proto + "_server_conn",
		ReadTimeout:       time.Duration(s.readTimeout),
		WriteTimeout:      time.Duration(s.writeTimeout),
		Encryption:        s.encryption,
		ServerCert:        s.serverCert,
		ServerKey:         s.serverKey,
		Handler:           router,
		Parent:            s,
	}
	err := s.inner.Initialize()
	if err != nil {
		return err
	}

	return nil
}

// Log implements logger.Writer.
func (s *httpServer) Log(level logger.Level, format string, args ...any) {
	s.parent.Log(level, format, args...)
}

func (s *httpServer) close() {
	s.inner.Close()
}

func (s *httpServer) middlewarePreflightRequests(ctx *gin.Context) {
	if ctx.Request.Method == http.MethodOptions &&
		ctx.Request.Header.Get("Access-Control-Request-Method") != "" {
		ctx.Header("Access-Control-Allow-Methods", "OPTIONS, GET")
		ctx.Header("Access-Control-Allow-Headers", "Authorization, Range")
		ctx.AbortWithStatus(http.StatusNoContent)
		return
	}
}

func (s *httpServer) writeErrorNoLog(ctx *gin.Context, status int, err error) {
	ctx.AbortWithStatusJSON(status, &defs.APIError{
		Status: defs.APIErrorStatusError,
		Error:  err.Error(),
	})
}

func (s *httpServer) findPathConf(ctx *gin.Context, dir string) (pathConf *conf.Path, abrChild bool, err error) {
	defer func() {
		if recover() != nil {
			pathConf = nil
			abrChild = false
			err = nil
		}
	}()

	pathConfName := dir
	if isABRChildPlaylistPath(dir) {
		pathConfName = abrBasePath(dir)
		abrChild = true
	}

	req := defs.PathFindPathConfReq{
		Author: &logger.InlineWriter{
			Parent: s,
			Prefix: fmt.Sprintf("[conn %v]", httpp.RemoteAddr(ctx)),
		},
		AccessRequest: defs.PathAccessRequest{
			Name:                 pathConfName,
			Query:                ctx.Request.URL.RawQuery,
			UserAgent:            ctx.Request.UserAgent(),
			Publish:              false,
			Proto:                auth.ProtocolHLS,
			Credentials:          httpp.Credentials(ctx.Request),
			IP:                   net.ParseIP(ctx.ClientIP()),
			EnableAskCredentials: true,
		},
	}

	res, err := s.pathManager.FindPathConf(req)
	if err != nil && !abrChild && strings.Contains(dir, "/") {
		basePath := abrBasePath(dir)
		req.AccessRequest.Name = basePath
		candidateRes, candidateErr := s.pathManager.FindPathConf(req)
		if candidateErr == nil && isConfiguredABRChildPath(dir, candidateRes.Conf) {
			return candidateRes.Conf, true, nil
		}
	}
	if err != nil {
		return nil, false, err
	}

	return res.Conf, abrChild, nil
}

func (s *httpServer) handleAuthError(ctx *gin.Context, err error) bool {
	if terr, ok := errors.AsType[*auth.Error](err); ok {
		if terr.AskCredentials {
			ctx.Header("WWW-Authenticate", `Basic realm="mediamtx"`)
		}
		s.writeErrorNoLog(ctx, http.StatusUnauthorized, fmt.Errorf("authentication error"))
		return true
	}
	return false
}

// abrChild resolves a request for an output of an ABR stream: a rendition the
// transcoder publishes (<path>/720) or the source (<path>/original). It
// returns the path of the muxer that serves it - the rendition's own, or the
// base path's for the source. ok is false for anything else, including a path
// that only looks like a rendition on a path without transcoding.
//
// ABR outputs are served without HLS sessions. The master playlist that links
// them is rendered here, not by a muxer, so there is no session to carry into
// their URIs, and a session-bound media playlist or segment would answer 401.
// Read access is checked on every request instead, and the muxer still counts
// the bytes it serves.
func (s *httpServer) abrChild(ctx *gin.Context, dir string) (muxerPath string, ok bool, err error) {
	if !isABRChildPlaylistPath(dir) {
		return "", false, nil
	}

	pathConf, _, err := s.findPathConf(ctx, dir)
	if err != nil {
		return "", false, err
	}

	if !isConfiguredABRChildPath(dir, pathConf) {
		return "", false, nil
	}

	if isABRSourceChild(dir) {
		return abrBasePath(dir), true, nil
	}
	return dir, true, nil
}

// abrRenditionWarmup is how long after a stream starts its master playlist
// lists every rendition, published or not: the transcoder needs a few seconds
// to publish them, and players read the master playlist once, so a rendition
// left out of it at the start would never be played.
var abrRenditionWarmup = 30 * time.Second

// liveABRRenditions returns the renditions the master playlist of the stream
// at basePath lists: after abrRenditionWarmup, only those served here. A
// rendition the transcoder lost is left out, so that new viewers do not pick
// a variant that answers an empty playlist until the transcoder is back.
// Viewers already on it keep reloading it and resume when it is.
func (s *httpServer) liveABRRenditions(
	basePath string,
	base *muxer,
	renditions []conf.HLSTranscodingRendition,
) []conf.HLSTranscodingRendition {
	if base == nil || time.Since(base.created) < abrRenditionWarmup {
		return renditions
	}

	var live []conf.HLSTranscodingRendition
	for _, r := range renditions {
		_, err := s.parent.getMuxer(serverGetMuxerReq{path: basePath + "/" + r.Name, create: false})
		if err == nil {
			live = append(live, r)
		}
	}
	return live
}

// serveABRChild serves a file of an ABR output of the stream at basePath
// from its muxer.
//
// Each output is a variant of the master playlist, so its index.m3u8 is its
// video media playlist - a master cannot name another master, which video.js
// does not accept - and the audio comes from the master's audio group.
//
// While the stream is live here but an output has no media playlist yet - the
// transcoder takes a few seconds to publish a rendition, and a muxer a segment
// to write one - playlists answer with an empty live playlist, so that players
// retry instead of stopping on a 404. When the stream is not live here at all
// the answer is 404, so that a proxy in front of several servers (HAProxy
// retries on 404) asks the one that has it.
func (s *httpServer) serveABRChild(ctx *gin.Context, basePath string, muxerPath string, fname string, isCDN bool) {
	isPlaylist := strings.HasSuffix(fname, ".m3u8")

	muxer, err := s.parent.getMuxer(serverGetMuxerReq{path: muxerPath, create: false})
	if err != nil {
		if isPlaylist && muxerPath != basePath {
			if _, err2 := s.parent.getMuxer(serverGetMuxerReq{path: basePath, create: false}); err2 == nil {
				writeHLSPlaceholderPlaylist(ctx)
				return
			}
		}
		s.writeErrorNoLog(ctx, http.StatusNotFound, err)
		return
	}

	if isPlaylist {
		mi := muxer.getInstance()
		if mi == nil || !mi.isMediaPlaylistReady() {
			writeHLSPlaceholderPlaylist(ctx)
			return
		}
		if fname == "index.m3u8" {
			fname = mi.primaryVideoPlaylist()
		}
	}

	ctx.Request.URL.Path = fname

	err = muxer.handleRequest(ctx, isCDN)
	if err != nil {
		if isPlaylist {
			writeHLSPlaceholderPlaylist(ctx)
			return
		}
		s.writeErrorNoLog(ctx, http.StatusInternalServerError, err)
	}
}

func (s *httpServer) onRequest(ctx *gin.Context) {
	if ctx.Request.Method != http.MethodGet {
		return
	}

	pa := ctx.Request.URL.Path[1:]
	if strings.HasPrefix(pa, "media/") {
		s.onMediaRequest(ctx, strings.TrimPrefix(pa, "media/"))
		return
	}

	var dir string
	var fname string

	type contentType int

	const (
		index contentType = iota
		multivariantPlaylist
		mediaPlaylist
		segment
	)

	var contentTyp contentType

	switch {
	case strings.HasSuffix(pa, "/hls.min.js"):
		ctx.Header("Cache-Control", "max-age=3600")
		ctx.Header("Content-Type", "application/javascript")
		ctx.Writer.WriteHeader(http.StatusOK)
		ctx.Writer.Write(hlsMinJS)
		return

	case pa == "", pa == "favicon.ico", strings.HasSuffix(pa, "/hls.min.js.map"):
		return

	case strings.HasSuffix(pa, ".m3u8"):
		dir, fname = path.Dir(pa), path.Base(pa)

		if fname == "index.m3u8" {
			contentTyp = multivariantPlaylist
		} else {
			contentTyp = mediaPlaylist
		}

	case strings.HasSuffix(pa, ".ts") ||
		strings.HasSuffix(pa, ".mp4") ||
		strings.HasSuffix(pa, ".m4s") ||
		strings.HasSuffix(pa, ".mp"):
		dir, fname = path.Dir(pa), path.Base(pa)

		if strings.HasSuffix(fname, ".mp") {
			fname += "4"
		}

		contentTyp = segment

	default:
		dir = pa

		if !strings.HasSuffix(dir, "/") {
			ctx.Header("Location", trailingSlashLocation(ctx.Request.URL.Path, ctx.Request.URL.RawQuery))
			ctx.Writer.WriteHeader(http.StatusFound)
			return
		}

		dir = dir[:len(dir)-1]
		contentTyp = index
	}

	isCDN := (s.cdnSecret != "" && ctx.Request.Header.Get("Authorization") == "Bearer "+s.cdnSecret)

	if contentTyp != index {
		muxerPath, ok, err := s.abrChild(ctx, dir)
		if err != nil && s.handleAuthError(ctx, err) {
			return
		}
		if ok {
			s.serveABRChild(ctx, abrBasePath(dir), muxerPath, fname, isCDN)
			return
		}
	}

	switch contentTyp {
	case index:
		_, _, err := s.findPathConf(ctx, dir)
		if err != nil {
			if s.handleAuthError(ctx, err) {
				return
			}
			s.writeErrorNoLog(ctx, http.StatusInternalServerError, err)
			return
		}

		ctx.Header("Cache-Control", "max-age=3600")
		ctx.Header("Content-Type", "text/html")
		ctx.Writer.WriteHeader(http.StatusOK)
		ctx.Writer.Write(hlsIndex)

	case multivariantPlaylist:
		pathConf, _, err := s.findPathConf(ctx, dir)
		if err != nil {
			pathConf = nil
		}

		if shouldRenderABRMaster(dir, pathConf) {
			var mi *muxerInstance
			mux, err := s.parent.getMuxer(serverGetMuxerReq{
				path:           dir,
				remoteAddr:     httpp.RemoteAddr(ctx),
				query:          ctx.Request.URL.RawQuery,
				sourceOnDemand: pathConf.SourceOnDemand,
			})
			if err == nil && mux != nil {
				mi = mux.getInstance()
			}

			hasAudio := true
			if mi != nil {
				hasAudio = mi.hasAudio()
			}

			ctx.Header("Cache-Control", "no-cache")
			ctx.Header("Content-Type", "application/vnd.apple.mpegurl")
			ctx.Writer.WriteHeader(http.StatusOK)
			ctx.Writer.Write(renderABRMasterPlaylist(
				s.liveABRRenditions(dir, mux, masterPlaylistRenditions(pathConf, mi)),
				codecStringForTranscodedOutput(pathConf, nil),
				hasAudio))
			return
		}

		if s.parent.DVREnabled && s.parent.DVRService != nil {
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

		if isCDN {
			if existingMuxer, err := s.parent.getMuxer(serverGetMuxerReq{path: dir, create: false}); err == nil {
				if sx := existingMuxer.getCDNSession(); sx != nil {
					sx.lastRequestTime.Store(time.Now().UnixNano())

					ctx.Writer = &responseWriterCounter{
						ResponseWriter: ctx.Writer,
						bytesSent:      &sx.bytesSent,
					}
					ctx.Request.URL.Path = fname

					err = existingMuxer.handleRequest(ctx, isCDN)
					if err != nil {
						s.writeErrorNoLog(ctx, http.StatusInternalServerError, err)
					}
					return
				}
			}

			sx := &session{
				isCDN:           true,
				remoteAddr:      httpp.RemoteAddr(ctx),
				pathName:        dir,
				externalCmdPool: s.parent.ExternalCmdPool,
				pathManager:     s.pathManager,
				server:          s.parent,
			}
			err := sx.initialize(ctx)
			if err != nil {
				if _, ok := errors.AsType[*defs.PathNoStreamAvailableError](err); ok {
					s.writeErrorNoLog(ctx, http.StatusNotFound, err)
					return
				}

				s.writeErrorNoLog(ctx, http.StatusInternalServerError, err)
				return
			}

			ctx.Writer = &responseWriterCounter{
				ResponseWriter: ctx.Writer,
				bytesSent:      &sx.bytesSent,
			}
			ctx.Request.URL.Path = fname

			err = sx.muxer.handleRequest(ctx, isCDN)
			if err != nil {
				s.writeErrorNoLog(ctx, http.StatusInternalServerError, err)
			}
			return
		}

		if ctx.Request.URL.Query().Get("cookieCheck") != "1" {
			http.SetCookie(ctx.Writer, &http.Cookie{
				Name:        "cookieCheck",
				Value:       "1",
				SameSite:    http.SameSiteNoneMode,
				Secure:      true,
				Partitioned: true,
				HttpOnly:    true,
			})

			q := ctx.Request.URL.Query()
			q.Set("cookieCheck", "1")
			ctx.Request.URL.RawQuery = q.Encode()
			ctx.Writer.Header().Set("Location", sanitizeLocation(ctx.Request.URL.Path, ctx.Request.URL.RawQuery))

			ctx.Writer.WriteHeader(http.StatusFound)
			return
		}

		q := ctx.Request.URL.Query()
		q.Del("cookieCheck")
		ctx.Request.URL.RawQuery = q.Encode()

		sx := &session{
			remoteAddr:      httpp.RemoteAddr(ctx),
			pathName:        dir,
			externalCmdPool: s.parent.ExternalCmdPool,
			pathManager:     s.pathManager,
			server:          s.parent,
		}
		err = sx.initialize(ctx)
		if err != nil {
			if s.handleAuthError(ctx, err) {
				return
			}

			if _, ok := errors.AsType[*defs.PathNoStreamAvailableError](err); ok {
				s.writeErrorNoLog(ctx, http.StatusNotFound, err)
				return
			}

			s.writeErrorNoLog(ctx, http.StatusInternalServerError, err)
			return
		}

		if cookie, err2 := ctx.Request.Cookie("cookieCheck"); err2 == nil && cookie.Value == "1" {
			http.SetCookie(ctx.Writer, &http.Cookie{
				Name:        sessionCookieName,
				Value:       sx.secret.String(),
				SameSite:    http.SameSiteNoneMode,
				Secure:      true,
				Partitioned: true,
				HttpOnly:    true,
			})
		} else {
			q = ctx.Request.URL.Query()
			q.Set(sessionQueryParamName, sx.secret.String())
			ctx.Request.URL.RawQuery = q.Encode()
		}

		ctx.Writer = &responseWriterCounter{
			ResponseWriter: ctx.Writer,
			bytesSent:      &sx.bytesSent,
		}

		ctx.Request.URL.Path = fname

		err = sx.muxer.handleRequest(ctx, isCDN)
		if err != nil {
			s.writeErrorNoLog(ctx, http.StatusInternalServerError, err)
			return
		}

	default:
		muxer, err := s.parent.getMuxer(serverGetMuxerReq{
			path:   dir,
			create: false,
		})
		if err != nil {
			s.writeErrorNoLog(ctx, http.StatusUnauthorized, fmt.Errorf("authentication error"))
			return
		}

		var sx *session
		if isCDN {
			sx = muxer.getCDNSession()
		} else {
			sx = muxer.findSession(ctx)
		}
		if sx == nil {
			s.writeErrorNoLog(ctx, http.StatusUnauthorized, fmt.Errorf("authentication error"))
			return
		}

		if isCDN {
			sx.lastRequestTime.Store(time.Now().UnixNano())
		}

		ctx.Writer = &responseWriterCounter{
			ResponseWriter: ctx.Writer,
			bytesSent:      &sx.bytesSent,
		}

		ctx.Request.URL.Path = fname

		err = muxer.handleRequest(ctx, isCDN)
		if err != nil {
			s.writeErrorNoLog(ctx, http.StatusInternalServerError, err)
			return
		}
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

	_, _, err = s.findPathConf(ctx, streamID)
	if err != nil {
		ctx.Writer.WriteHeader(http.StatusNotFound)
		return
	}

	if !s.parent.DVREnabled || s.parent.DVRService == nil || !s.parent.DVRService.ServeMedia(ctx.Writer, ctx.Request, streamID, segmentName) {
		ctx.Writer.WriteHeader(http.StatusNotFound)
	}
}
