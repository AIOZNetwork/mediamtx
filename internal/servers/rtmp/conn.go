package rtmp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/bluenviron/gortmplib"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/google/uuid"

	"github.com/bluenviron/mediamtx/internal/auth"
	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/defs"
	"github.com/bluenviron/mediamtx/internal/externalcmd"
	"github.com/bluenviron/mediamtx/internal/hooks"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/protocols/rtmp"
	"github.com/bluenviron/mediamtx/internal/stream"
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

	ctx       context.Context
	ctxCancel func()
	uuid      uuid.UUID
	created   time.Time
	mutex     sync.RWMutex
	rconn     *gortmplib.ServerConn
	state     defs.APIRTMPConnState
	pathName  string
	streamKey string
	query     string
	user      string
	userAgent string
	reader    *stream.Reader
}

func (c *conn) initialize() {
	c.ctx, c.ctxCancel = context.WithCancel(c.parentCtx)

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

	var target *PublishTarget

	if c.parent.StreamKeys != nil {
		var err error
		target, err = c.parent.StreamKeys.ResolvePublish(c.ctx, c.rconn.URL, c.parent.isStreamKeyPublishing)
		if err != nil {
			c.rconn.RejectAction() //nolint:errcheck
			return err
		}

		// ResolvePublish only saw that the key was free; two publishers can
		// both get that far, and the claim lets exactly one through.
		if !c.parent.claimStreamKey(target.StreamKey) {
			c.rconn.RejectAction() //nolint:errcheck
			return errStreamKeyPublishing
		}
		defer c.parent.releaseStreamKey(target.StreamKey)

		pathName = target.PathName
	}

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

	if c.parent.PublishWebhook != "" {
		// The API reads the connection back through the control API while it
		// handles the webhook, so the path and key must be visible first.
		c.mutex.Lock()
		c.pathName = pathName
		c.streamKey = target.streamKey()
		c.mutex.Unlock()

		err = c.callPublishWebhook(pathName, target.streamKey())
		if err != nil {
			c.rconn.RejectAction() //nolint:errcheck
			return err
		}

		// The webhook may have used most of the read deadline set before the
		// handshake; the tracks are read next.
		c.nconn.SetReadDeadline(time.Now().Add(time.Duration(c.readTimeout)))
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

	// An audio stream key publishes its audio alone, whatever the encoder
	// sends.
	if target != nil && target.KeyType == liveStreamKeyTypeAudio {
		medias, err = dropVideo(r, medias)
		if err != nil {
			return err
		}
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
		StreamKey: target.streamKey(),
	})
	if err != nil {
		return err
	}

	defer res2.Path.RemovePublisher(defs.PathRemovePublisherReq{Author: c})

	subStream = res2.SubStream

	c.mutex.Lock()
	c.state = defs.APIRTMPConnStatePublish
	c.pathName = pathName
	c.streamKey = target.streamKey()
	c.query = c.rconn.URL.RawQuery
	c.user = res1.User
	c.mutex.Unlock()

	if target != nil {
		mediaID, err2 := uuid.Parse(target.PathName)
		if err2 != nil {
			return err2
		}

		stats := &publishStatistics{
			mediaID:       mediaID,
			bytesReceived: c.rconn.BytesReceived,
			repo:          c.parent.StreamKeys.Statistics(),
			parent:        c,
		}
		statsCtx, statsCancel := context.WithCancel(c.ctx)
		statsDone := make(chan struct{})
		go func() {
			defer close(statsDone)
			stats.run(statsCtx, subStream.Stream, medias)
		}()
		defer func() {
			statsCancel()
			<-statsDone
		}()
	}

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
		StreamKey:               c.streamKey,
	}
}
