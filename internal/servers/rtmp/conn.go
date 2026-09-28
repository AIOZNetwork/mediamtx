package rtmp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/bluenviron/gortmplib"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/bluenviron/mediamtx/internal/auth"
	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/database"
	"github.com/bluenviron/mediamtx/internal/database/repository"
	"github.com/bluenviron/mediamtx/internal/defs"
	"github.com/bluenviron/mediamtx/internal/externalcmd"
	"github.com/bluenviron/mediamtx/internal/hooks"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/protocols/rtmp"
	"github.com/bluenviron/mediamtx/internal/stream"
)

func (c *conn) pathNameAndQuery(inURL *url.URL, isPublish bool, listStreamKey *map[string]bool) (string, url.Values, string, string, error) {
	tmp := strings.TrimRight(inURL.String(), "/")
	ur, _ := url.Parse(tmp)
	pathName := strings.TrimLeft(ur.Path, "/")

	if !isPublish {
		return pathName, ur.Query(), ur.RawQuery, "", nil
	}

	streamKeyStr := pathName
	if idx := strings.LastIndex(pathName, "/"); idx != -1 {
		streamKeyStr = pathName[idx+1:]
	}

	if listStreamKey != nil && (*listStreamKey)[streamKeyStr] {
		return "", nil, "", "", errors.New("this streamkey is streaming")
	}

	if streamKeyStr == "" {
		return "", nil, "", "", errors.New("invalid path name")
	}
	uuidPathName, err := uuid.Parse(streamKeyStr)
	if err != nil {
		return "", nil, "", "", errors.New("invalid path name")
	}

	videoStreaming, err := c.livestreamVideoRepo.GetStreamMediaAvaialbleByStreamKey(uuidPathName)
	if err != nil && err != gorm.ErrRecordNotFound {
		return "", nil, "", "", errors.New("something went wrong")
	}

	if err == gorm.ErrRecordNotFound { // stream directly without create stream session

		streamKey := c.livestreamVideoRepo.GetStreamKeyExist(uuidPathName)
		if streamKey == uuid.Nil {
			return "", nil, "", "", errors.New("invalid path name")
		}

		newStreamID := uuid.New()

		return newStreamID.String(), ur.Query(), ur.RawQuery, streamKeyStr, nil
	}

	if videoStreaming.Status == "streaming" {
		value, _ := database.RedisIdDb.Get(c.ctx, videoStreaming.Id.String()).Result()
		if value != "" {
			// If Redis says this server is streaming it, but listStreamKey does not have it,
			// then the previous session on this server crashed or was terminated without clean disconnect.
			if value == conf.IdentityServer && (listStreamKey == nil || !(*listStreamKey)[streamKeyStr]) {
				_ = database.RedisIdDb.Del(c.ctx, videoStreaming.Id.String()).Err()
				_ = c.livestreamVideoRepo.UpdateStreamMediaStatus(videoStreaming.Id, "ended")

				newStreamID := uuid.New()
				return newStreamID.String(), ur.Query(), ur.RawQuery, streamKeyStr, nil
			}
			return "", nil, "", "", errors.New("this streamkey is streaming")
		}
	}

	return videoStreaming.Id.String(), ur.Query(), ur.RawQuery, streamKeyStr, nil
}

type connState int

const (
	connStateRead connState = iota + 1
	connStatePublish
)

type conn struct {
	parentCtx           context.Context
	encryption          bool
	rtspAddress         string
	readTimeout         conf.Duration
	writeTimeout        conf.Duration
	runOnConnect        string
	runOnConnectRestart bool
	runOnDisconnect     string
	wg                  *sync.WaitGroup
	nconn               net.Conn
	externalCmdPool     *externalcmd.Pool
	pathManager         serverPathManager
	parent              *Server
	livestreamVideoRepo *repository.LiveStreamVideoRepository

	ctx       context.Context
	ctxCancel func()
	uuid      uuid.UUID
	created   time.Time
	mutex     sync.RWMutex
	rconn     *gortmplib.ServerConn
	state     defs.APIRTMPConnState
	pathName  string
	query     string
	user      string
	userAgent string
	reader    *stream.Reader
}

func (c *conn) initialize() {
	c.ctx, c.ctxCancel = context.WithCancel(c.parentCtx)
	c.livestreamVideoRepo = repository.NewLiveStreamVideoRepository(database.DB)

	c.uuid = uuid.New()
	c.created = time.Now()
	c.state = defs.APIRTMPConnStateIdle

	c.Log(logger.Info, "opened")

	c.wg.Add(1)
	go c.run()
}

func (c *conn) Close() {
	c.ctxCancel()
}

func (c *conn) remoteAddr() net.Addr {
	return c.nconn.RemoteAddr()
}

// Log implements logger.Writer.
func (c *conn) Log(level logger.Level, format string, args ...any) {
	c.parent.Log(level, "[conn %v] "+format, append([]any{c.nconn.RemoteAddr()}, args...)...)
}

func (c *conn) ip() net.IP {
	return c.nconn.RemoteAddr().(*net.TCPAddr).IP
}

func (c *conn) run() { //nolint:dupl
	defer c.wg.Done()

	onDisconnectHook := hooks.OnConnect(hooks.OnConnectParams{
		Logger:              c,
		ExternalCmdPool:     c.externalCmdPool,
		RunOnConnect:        c.runOnConnect,
		RunOnConnectRestart: c.runOnConnectRestart,
		RunOnDisconnect:     c.runOnDisconnect,
		RTSPAddress:         c.rtspAddress,
		Desc:                *c.APIReaderDescribe(),
	})
	defer onDisconnectHook()

	err := c.runInner()

	c.ctxCancel()

	c.parent.closeConn(c)

	c.Log(logger.Info, "closed: %v", err)
}

func (c *conn) runInner() error {
	readerErr := make(chan error)
	go func() {
		readerErr <- c.runReader()
	}()

	select {
	case err := <-readerErr:
		c.nconn.Close()
		return err

	case <-c.ctx.Done():
		c.nconn.Close()
		<-readerErr
		return errors.New("terminated")
	}
}

func (c *conn) runReader() error {
	c.nconn.SetReadDeadline(time.Now().Add(time.Duration(c.readTimeout)))
	c.nconn.SetWriteDeadline(time.Now().Add(time.Duration(c.writeTimeout)))

	conn := &gortmplib.ServerConn{
		RW: c.nconn,
	}
	err := conn.Initialize()
	if err != nil {
		return err
	}

	err = conn.AcceptConn()
	if err != nil {
		return err
	}

	c.mutex.Lock()
	c.rconn = conn
	c.userAgent = conn.FlashVer
	c.mutex.Unlock()

	if !conn.Publish {
		return c.runRead()
	}
	return c.runPublish()
}

func (c *conn) runRead() error {
	pathName := strings.TrimLeft(c.rconn.URL.Path, "/")
	query := c.rconn.URL.Query()

	res, err := c.pathManager.AddReader(defs.PathAddReaderReq{
		Author: c,
		AccessRequest: defs.PathAccessRequest{
			Name:      pathName,
			Query:     c.rconn.URL.RawQuery,
			UserAgent: c.userAgent,
			Proto:     auth.ProtocolRTMP,
			ID:        &c.uuid,
			Credentials: &auth.Credentials{
				User: query.Get("user"),
				Pass: query.Get("pass"),
			},
			IP:                   c.ip(),
			EnableAskCredentials: false,
		},
	})
	if err != nil {
		if _, ok := errors.AsType[*auth.Error](err); ok {
			rejectErr := c.rconn.RejectAction()
			if rejectErr != nil {
				return rejectErr
			}
		}

		return err
	}

	err = c.rconn.AcceptAction()
	if err != nil {
		return err
	}

	defer res.Path.RemoveReader(defs.PathRemoveReaderReq{Author: c})

	c.mutex.Lock()
	c.state = defs.APIRTMPConnStateRead
	c.pathName = pathName
	c.query = c.rconn.URL.RawQuery
	c.user = res.User
	c.mutex.Unlock()

	r := &stream.Reader{Parent: c}

	err = rtmp.FromStream(
		res.Stream.OrigDesc,
		res.Stream.OutDescCopy(),
		r,
		c.rconn,
		c.nconn,
		time.Duration(c.writeTimeout),
		c.rconn.FourCcList)
	if err != nil {
		return err
	}

	c.Log(logger.Info, "is reading from path '%s', %s",
		res.Path.Name(), defs.FormatsInfo(r.Formats()))

	onUnreadHook := hooks.OnRead(hooks.OnReadParams{
		Logger:          c,
		ExternalCmdPool: c.externalCmdPool,
		Conf:            res.Path.SafeConf(),
		ExternalCmdEnv:  res.Path.ExternalCmdEnv(),
		Reader:          *c.APIReaderDescribe(),
		Query:           c.rconn.URL.RawQuery,
	})
	defer onUnreadHook()

	c.nconn.SetReadDeadline(time.Time{})

	res.Stream.AddReader(r)
	defer res.Stream.RemoveReader(r)

	c.mutex.Lock()
	c.reader = r
	c.mutex.Unlock()

	select {
	case <-c.ctx.Done():
		return fmt.Errorf("terminated")

	case err = <-r.Error():
		return err
	}
}

func (c *conn) runPublish() error {
	pathName := strings.TrimLeft(c.rconn.URL.Path, "/")
	query := c.rconn.URL.Query()

	res1, err := c.pathManager.FindPathConf(defs.PathFindPathConfReq{
		Author: c,
		AccessRequest: defs.PathAccessRequest{
			Name:      pathName,
			Query:     c.rconn.URL.RawQuery,
			Publish:   true,
			UserAgent: c.userAgent,
			Proto:     auth.ProtocolRTMP,
			ID:        &c.uuid,
			Credentials: &auth.Credentials{
				User: query.Get("user"),
				Pass: query.Get("pass"),
			},
			IP:                   c.ip(),
			EnableAskCredentials: false,
		},
	})
	if err != nil {
		if _, ok := errors.AsType[*auth.Error](err); ok {
			rejectErr := c.rconn.RejectAction()
			if rejectErr != nil {
				return rejectErr
			}
		}

		return err
	}

	err = c.rconn.AcceptAction()
	if err != nil {
		return err
	}

	r := &gortmplib.Reader{
		Conn: c.rconn,
	}
	err = r.Initialize()
	if err != nil {
		return err
	}

	var subStream *stream.SubStream

	medias, err := rtmp.ToStream(r, &subStream)
	if err != nil {
		return err
	}

	res2, err := c.pathManager.AddPublisher(defs.PathAddPublisherReq{
		Author:        c,
		Desc:          &description.Session{Medias: medias},
		UseRTPPackets: false,
		ReplaceNTP:    true,
		ConfToCompare: res1.Conf,
		AccessRequest: defs.PathAccessRequest{
			Name:     pathName,
			Query:    c.rconn.URL.RawQuery,
			Publish:  true,
			SkipAuth: true,
		},
	})
	if err != nil {
		return err
	}

	defer res2.Path.RemovePublisher(defs.PathRemovePublisherReq{Author: c})

	subStream = res2.SubStream

	c.mutex.Lock()
	c.state = defs.APIRTMPConnStatePublish
	c.pathName = pathName
	c.query = c.rconn.URL.RawQuery
	c.user = res1.User
	c.mutex.Unlock()

	c.nconn.SetWriteDeadline(time.Time{})

	for {
		c.nconn.SetReadDeadline(time.Now().Add(time.Duration(c.readTimeout)))
		err = r.Read()
		if err != nil {
			return err
		}
	}
}

// APIReaderDescribe implements reader.
func (c *conn) APIReaderDescribe() *defs.APIPathReader {
	return &defs.APIPathReader{
		Type: func() defs.APIPathReaderType {
			if c.encryption {
				return defs.APIPathReaderTypeRTMPSConn
			}
			return defs.APIPathReaderTypeRTMPConn
		}(),
		ID: c.uuid.String(),
	}
}

// APISourceDescribe implements source.
func (c *conn) APISourceDescribe() *defs.APIPathSource {
	return &defs.APIPathSource{
		Type: func() defs.APIPathSourceType {
			if c.encryption {
				return defs.APIPathSourceTypeRTMPSConn
			}
			return defs.APIPathSourceTypeRTMPConn
		}(),
		ID: c.uuid.String(),
	}
}

func (c *conn) apiItem() *defs.APIRTMPConn {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	bytesReceived := uint64(0)
	bytesSent := uint64(0)
	outboundFramesDiscarded := uint64(0)

	if c.rconn != nil {
		bytesReceived = c.rconn.BytesReceived()
		bytesSent = c.rconn.BytesSent()
	}

	if c.reader != nil {
		outboundFramesDiscarded = c.reader.OutboundFramesDiscarded()
	}

	return &defs.APIRTMPConn{
		ID:                      c.uuid,
		Created:                 c.created,
		RemoteAddr:              c.remoteAddr().String(),
		State:                   c.state,
		Path:                    c.pathName,
		Query:                   c.query,
		User:                    c.user,
		UserAgent:               c.userAgent,
		InboundBytes:            bytesReceived,
		OutboundBytes:           bytesSent,
		BytesReceived:           bytesReceived,
		BytesSent:               bytesSent,
		OutboundFramesDiscarded: outboundFramesDiscarded,
	}
}
