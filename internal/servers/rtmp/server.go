// Package rtmp contains a RTMP server.
package rtmp

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/bluenviron/mediamtx/internal/certloader"
	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/defs"
	"github.com/bluenviron/mediamtx/internal/externalcmd"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/restrictnetwork"
	"github.com/bluenviron/mediamtx/internal/stream"
)

// ErrConnNotFound is returned when a connection is not found.
var ErrConnNotFound = errors.New("connection not found")

type serverAPICreateStreamKeyRes struct {
	err error
}

type serverAPICreateStreamKeyReq struct {
	streamKey uuid.UUID
	streamId  uuid.UUID
	available bool
	res       chan serverAPICreateStreamKeyRes
}

type serverAPIDeleteStreamIdRes struct {
	err error
}

type serverAPIDeleteStreamIdReq struct {
	streamKey uuid.UUID
	streamId  uuid.UUID
	res       chan serverAPIDeleteStreamIdRes
}

type serverAPIDeleteStreamKeyRes struct {
	err error
}

type serverAPIDeleteStreamKeyReq struct {
	streamKey uuid.UUID
	res       chan serverAPIDeleteStreamKeyRes
}

type serverAPIConnsListRes struct {
	data *defs.APIRTMPConnList
	err  error
}

type serverAPIConnsListReq struct {
	res chan serverAPIConnsListRes
}

type serverAPIConnsGetRes struct {
	data *defs.APIRTMPConn
	err  error
}

type serverAPIConnsGetReq struct {
	uuid uuid.UUID
	res  chan serverAPIConnsGetRes
}

type serverAPIConnsKickRes struct {
	err error
}

type StreamInfo struct {
	ConnID       string `json:"connId"`
	StreamKey    string `json:"streamKey"`
	StreamID     string `json:"streamId"`
	Available    bool   `json:"available"`
	CreatedAt    string `json:"createdAt"`
	DirectStream bool   `json:"directStream"`
}

type serverAPIConnsKickReq struct {
	uuid uuid.UUID
	res  chan serverAPIConnsKickRes
}

type serverPathManager interface {
	AddPublisher(req defs.PathAddPublisherReq) (defs.Path, error)
	AddReader(req defs.PathAddReaderReq) (defs.Path, *stream.Stream, error)
}

type serverParent interface {
	logger.Writer
}

// Server is a RTMP server.
type Server struct {
	Address             string
	ReadTimeout         conf.Duration
	WriteTimeout        conf.Duration
	IsTLS               bool
	ServerCert          string
	ServerKey           string
	RTSPAddress         string
	RunOnConnect        string
	RunOnConnectRestart bool
	RunOnDisconnect     string
	ExternalCmdPool     *externalcmd.Pool
	PathManager         serverPathManager
	Parent              serverParent

	ctx       context.Context
	ctxCancel func()
	wg        sync.WaitGroup
	ln        net.Listener
	conns     map[*conn]struct{}
	loader    *certloader.CertLoader

	// in
	chNewConn            chan net.Conn
	chAcceptErr          chan error
	chCloseConn          chan *conn
	chAPIConnsList       chan serverAPIConnsListReq
	chAPIConnsGet        chan serverAPIConnsGetReq
	chAPIConnsKick       chan serverAPIConnsKickReq
	chAPICreateStreamKey chan serverAPICreateStreamKeyReq
	chAPIDeleteStreamId  chan serverAPIDeleteStreamIdReq
	chAPIDeleteStreamKey chan serverAPIDeleteStreamKeyReq

	streamOpMutex sync.Mutex
}

// Initialize initializes the server.
func (s *Server) Initialize() error {
	ln, err := func() (net.Listener, error) {
		if !s.IsTLS {
			return net.Listen(restrictnetwork.Restrict("tcp", s.Address))
		}

		var err error
		s.loader, err = certloader.New(s.ServerCert, s.ServerKey, s.Parent)
		if err != nil {
			return nil, err
		}

		network, address := restrictnetwork.Restrict("tcp", s.Address)
		return tls.Listen(network, address, &tls.Config{GetCertificate: s.loader.GetCertificate()})
	}()
	if err != nil {
		return err
	}

	s.ctx, s.ctxCancel = context.WithCancel(context.Background())

	s.ln = ln
	s.conns = make(map[*conn]struct{})
	s.chNewConn = make(chan net.Conn)
	s.chAcceptErr = make(chan error)
	s.chCloseConn = make(chan *conn)
	s.chAPIConnsList = make(chan serverAPIConnsListReq)
	s.chAPIConnsGet = make(chan serverAPIConnsGetReq)
	s.chAPIConnsKick = make(chan serverAPIConnsKickReq)
	s.chAPICreateStreamKey = make(chan serverAPICreateStreamKeyReq)
	s.chAPIDeleteStreamId = make(chan serverAPIDeleteStreamIdReq)
	s.chAPIDeleteStreamKey = make(chan serverAPIDeleteStreamKeyReq)
	s.Log(logger.Info, "listener opened on %s", s.Address)

	l := &listener{
		ln:     s.ln,
		wg:     &s.wg,
		parent: s,
	}
	l.initialize()

	s.wg.Add(1)
	go s.run()

	return nil
}

// Log implements logger.Writer.
func (s *Server) Log(level logger.Level, format string, args ...interface{}) {
	label := func() string {
		if s.IsTLS {
			return "RTMPS"
		}
		return "RTMP"
	}()
	s.Parent.Log(level, "[%s] "+format, append([]interface{}{label}, args...)...)
}

// Close closes the server.
func (s *Server) Close() {
	s.Log(logger.Info, "listener is closing")
	s.ctxCancel()
	s.wg.Wait()
	if s.loader != nil {
		s.loader.Close()
	}
}

func (s *Server) run() {
	defer s.wg.Done()

	filename := "/app/streamId/streamId.json"
outer:
	for {
		select {
		case err := <-s.chAcceptErr:
			s.Log(logger.Error, "%s", err)
			break outer

		case nconn := <-s.chNewConn:
			c := &conn{
				parentCtx:           s.ctx,
				isTLS:               s.IsTLS,
				rtspAddress:         s.RTSPAddress,
				readTimeout:         s.ReadTimeout,
				writeTimeout:        s.WriteTimeout,
				runOnConnect:        s.RunOnConnect,
				runOnConnectRestart: s.RunOnConnectRestart,
				runOnDisconnect:     s.RunOnDisconnect,
				wg:                  &s.wg,
				nconn:               nconn,
				externalCmdPool:     s.ExternalCmdPool,
				pathManager:         s.PathManager,
				parent:              s,
			}
			c.initialize()
			s.conns[c] = struct{}{}

		case c := <-s.chCloseConn:
			delete(s.conns, c)

		case req := <-s.chAPIConnsList:
			data := &defs.APIRTMPConnList{
				Items: []*defs.APIRTMPConn{},
			}

			for c := range s.conns {
				data.Items = append(data.Items, c.apiItem())
			}

			sort.Slice(data.Items, func(i, j int) bool {
				return data.Items[i].Created.Before(data.Items[j].Created)
			})

			req.res <- serverAPIConnsListRes{data: data}

		case req := <-s.chAPIConnsGet:
			c := s.findConnByUUID(req.uuid)
			if c == nil {
				req.res <- serverAPIConnsGetRes{err: ErrConnNotFound}
				continue
			}

			item := c.apiItem()

			isDirectStream := false
			streamID := c.pathName

			filename := "/app/streamId/streamId.json"
			data, err := os.ReadFile(filename)
			if err == nil {
				var streams []StreamInfo
				if err := json.Unmarshal(data, &streams); err == nil {
					for _, stream := range streams {
						if stream.StreamID == streamID {
							isDirectStream = stream.DirectStream
							break
						}
					}
				}
			}

			if isDirectStream {
				var parsedStreamKey uuid.UUID
				maxRetries := 5
				retryDelay := 500 * time.Millisecond
				success := false

				for i := 0; i < maxRetries; i++ {
					fmt.Println("c.pathName", c.pathName)
					streamKey := findStreamKeyByStreamID(c.pathName)

					if streamKey == "" {
						c.Log(logger.Error, "Stream key is empty (attempt %d/%d), retrying...", i+1, maxRetries)
						time.Sleep(retryDelay)
						continue
					}

					var err error
					parsedStreamKey, err = uuid.Parse(streamKey)
					if err != nil {
						c.Log(logger.Error, "Error parsing stream key (attempt %d/%d): %v", i+1, maxRetries, err)
						if i < maxRetries-1 {
							time.Sleep(retryDelay)
						}
						continue
					}

					if parsedStreamKey != uuid.Nil {
						success = true
						break
					}

					c.Log(logger.Error, "Parsed stream key is nil (attempt %d/%d), retrying...", i+1, maxRetries)
					if i < maxRetries-1 {
						time.Sleep(retryDelay)
					}
				}

				if !success {
					c.Log(logger.Error, "Failed to get valid stream key after %d attempts", maxRetries)
				}
				item.StreamKeyId = parsedStreamKey
			}

			req.res <- serverAPIConnsGetRes{data: item}

		case req := <-s.chAPIConnsKick:
			c := s.findConnByUUID(req.uuid)
			if c == nil {
				req.res <- serverAPIConnsKickRes{err: ErrConnNotFound}
				continue
			}

			delete(s.conns, c)
			c.Close()
			req.res <- serverAPIConnsKickRes{}

		case req := <-s.chAPICreateStreamKey:
			func() {
				s.streamOpMutex.Lock()
				defer s.streamOpMutex.Unlock()

				var streams []StreamInfo
				if err := s.readJSONFile(filename, &streams); err != nil {
					if !os.IsNotExist(err) {
						req.res <- serverAPICreateStreamKeyRes{err: fmt.Errorf("read file: %w", err)}
						return
					}
					streams = []StreamInfo{}
				}

				streamExists := false
				for _, stream := range streams {
					if stream.StreamID == req.streamId.String() {
						streamExists = true
						break
					}
				}

				if streamExists {
					req.res <- serverAPICreateStreamKeyRes{err: fmt.Errorf("stream id already exists")}
					return
				}

				newStream := StreamInfo{
					StreamKey: req.streamKey.String(),
					StreamID:  req.streamId.String(),
					Available: req.available,
					CreatedAt: time.Now().Format(time.RFC3339),
				}

				streams = append(streams, newStream)

				if err := s.writeJSONFile(filename, streams); err != nil {
					req.res <- serverAPICreateStreamKeyRes{err: err}
					return
				}

				req.res <- serverAPICreateStreamKeyRes{err: nil}
			}()

		case req := <-s.chAPIDeleteStreamId:
			func() {
				s.streamOpMutex.Lock()
				defer s.streamOpMutex.Unlock()

				var streams []StreamInfo
				if err := s.readJSONFile(filename, &streams); err != nil {
					if os.IsNotExist(err) {
						req.res <- serverAPIDeleteStreamIdRes{err: fmt.Errorf("file not found")}
						return
					}
					req.res <- serverAPIDeleteStreamIdRes{err: fmt.Errorf("read file: %w", err)}
					return
				}

				found := false
				newStreams := []StreamInfo{}
				for _, stream := range streams {
					if stream.StreamKey == req.streamKey.String() && stream.StreamID == req.streamId.String() {
						found = true
						continue
					}
					newStreams = append(newStreams, stream)
				}

				if !found {
					req.res <- serverAPIDeleteStreamIdRes{err: fmt.Errorf("stream not found")}
					return
				}

				if err := s.writeJSONFile(filename, newStreams); err != nil {
					req.res <- serverAPIDeleteStreamIdRes{err: err}
					return
				}

				req.res <- serverAPIDeleteStreamIdRes{err: nil}
			}()

		case req := <-s.chAPIDeleteStreamKey:
			func() {
				s.streamOpMutex.Lock()
				defer s.streamOpMutex.Unlock()

				var streams []StreamInfo
				if err := s.readJSONFile(filename, &streams); err != nil {
					if os.IsNotExist(err) {
						req.res <- serverAPIDeleteStreamKeyRes{err: nil}
						return
					}
					req.res <- serverAPIDeleteStreamKeyRes{err: fmt.Errorf("read file: %w", err)}
					return
				}

				found := false
				newStreams := []StreamInfo{}
				for _, stream := range streams {
					if stream.StreamKey == req.streamKey.String() {
						found = true
						continue
					}
					newStreams = append(newStreams, stream)
				}

				if !found {
					req.res <- serverAPIDeleteStreamKeyRes{err: nil}
					return
				}

				if err := s.writeJSONFile(filename, newStreams); err != nil {
					req.res <- serverAPIDeleteStreamKeyRes{err: err}
					return
				}

				req.res <- serverAPIDeleteStreamKeyRes{err: nil}
			}()

		case <-s.ctx.Done():
			break outer
		}
	}

	s.ctxCancel()

	s.ln.Close()
}

func (s *Server) findConnByUUID(uuid uuid.UUID) *conn {
	for c := range s.conns {
		if c.uuid == uuid {
			return c
		}
	}
	return nil
}

// newConn is called by rtmpListener.
func (s *Server) newConn(conn net.Conn) {
	select {
	case s.chNewConn <- conn:
	case <-s.ctx.Done():
		conn.Close()
	}
}

// acceptError is called by rtmpListener.
func (s *Server) acceptError(err error) {
	select {
	case s.chAcceptErr <- err:
	case <-s.ctx.Done():
	}
}

// closeConn is called by conn.
func (s *Server) closeConn(c *conn) {
	select {
	case s.chCloseConn <- c:
	case <-s.ctx.Done():
	}
}

// APIConnsList is called by api.
func (s *Server) APIConnsList() (*defs.APIRTMPConnList, error) {
	req := serverAPIConnsListReq{
		res: make(chan serverAPIConnsListRes),
	}

	select {
	case s.chAPIConnsList <- req:
		res := <-req.res
		return res.data, res.err

	case <-s.ctx.Done():
		return nil, fmt.Errorf("terminated")
	}
}

// APIConnsGet is called by api.
func (s *Server) APIConnsGet(uuid uuid.UUID) (*defs.APIRTMPConn, error) {
	req := serverAPIConnsGetReq{
		uuid: uuid,
		res:  make(chan serverAPIConnsGetRes),
	}

	select {
	case s.chAPIConnsGet <- req:
		res := <-req.res
		return res.data, res.err

	case <-s.ctx.Done():
		return nil, fmt.Errorf("terminated")
	}
}

// APIConnsKick is called by api.
func (s *Server) APIConnsKick(uuid uuid.UUID) error {
	req := serverAPIConnsKickReq{
		uuid: uuid,
		res:  make(chan serverAPIConnsKickRes),
	}

	select {
	case s.chAPIConnsKick <- req:
		res := <-req.res
		return res.err

	case <-s.ctx.Done():
		return fmt.Errorf("terminated")
	}
}

// APICreateStreamKey is called by api.
func (s *Server) APICreateStreamKey(streamKey uuid.UUID, streamId uuid.UUID) (uuid.UUID, error) {
	availableDefault := true
	req := serverAPICreateStreamKeyReq{
		streamKey: streamKey,
		streamId:  streamId,
		available: availableDefault,
		res:       make(chan serverAPICreateStreamKeyRes),
	}

	select {
	case s.chAPICreateStreamKey <- req:
		res := <-req.res
		return req.streamId, res.err

	case <-s.ctx.Done():
		return uuid.Nil, fmt.Errorf("terminated")
	}
}

// APIDeleteStreamId is called by api.
func (s *Server) APIDeleteStreamId(streamKey uuid.UUID, streamId uuid.UUID) error {
	req := serverAPIDeleteStreamIdReq{
		streamKey: streamKey,
		streamId:  streamId,
		res:       make(chan serverAPIDeleteStreamIdRes),
	}

	select {
	case s.chAPIDeleteStreamId <- req:
		res := <-req.res
		return res.err

	case <-s.ctx.Done():
		return fmt.Errorf("terminated")
	}
}

// APIDeleteStreamKey is called by api.
func (s *Server) APIDeleteStreamKey(streamKey uuid.UUID) error {
	req := serverAPIDeleteStreamKeyReq{
		streamKey: streamKey,
		res:       make(chan serverAPIDeleteStreamKeyRes),
	}

	select {
	case s.chAPIDeleteStreamKey <- req:
		res := <-req.res
		return res.err

	case <-s.ctx.Done():
		return fmt.Errorf("terminated")
	}
}

func (s *Server) readJSONFile(filename string, streams *[]StreamInfo) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	return decoder.Decode(streams)
}

func (s *Server) writeJSONFile(filename string, streams []StreamInfo) error {
	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}

	updatedData, err := json.MarshalIndent(streams, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal streams: %w", err)
	}

	if err := os.WriteFile(filename, updatedData, 0644); err != nil {
		return fmt.Errorf("write file: %w", err)
	}

	return nil
}

func findStreamKeyByStreamID(streamID string) string {
	filename := "/app/streamId/streamId.json"
	data, err := os.ReadFile(filename)
	if err != nil {
		return ""
	}

	var streams []StreamInfo
	if err := json.Unmarshal(data, &streams); err != nil {
		fmt.Println("Error unmarshalling data: ", err)
		return ""
	}

	for _, stream := range streams {
		if stream.StreamID == streamID {
			return stream.StreamKey
		}
	}

	return ""
}
