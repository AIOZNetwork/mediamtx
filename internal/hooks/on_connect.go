package hooks

import (
	"net"
	"strconv"
	"strings"

	"github.com/bluenviron/mediamtx/internal/defs"
	"github.com/bluenviron/mediamtx/internal/externalcmd"
	"github.com/bluenviron/mediamtx/internal/logger"
)

// IsTranscoderChildPath returns whether a path is an internal HLS transcoder
// output path published back into RTSP by FFmpeg.
func IsTranscoderChildPath(pathName string) bool {
	if !strings.Contains(pathName, "/") {
		return false
	}

	if strings.Contains(pathName, "/video/") || strings.Contains(pathName, "/audio/") {
		return true
	}

	lastPart := pathName[strings.LastIndex(pathName, "/")+1:]
	if lastPart == "original" || lastPart == "main" {
		return true
	}

	if _, err := strconv.Atoi(strings.TrimSuffix(lastPart, "p")); err == nil {
		return true
	}

	return false
}

// OnConnectParams are the parameters of OnConnect.
type OnConnectParams struct {
	Logger              logger.Writer
	ExternalCmdPool     *externalcmd.Pool
	RunOnConnect        string
	RunOnConnectRestart bool
	RunOnDisconnect     string
	RTSPAddress         string
	Desc                defs.APIPathReader
}

// OnConnect is the OnConnect hook.
func OnConnect(params OnConnectParams) func() {
	var env externalcmd.Environment
	var onConnectCmd *externalcmd.Cmd

	if params.RunOnConnect != "" || params.RunOnDisconnect != "" {
		_, port, _ := net.SplitHostPort(params.RTSPAddress)
		env = externalcmd.Environment{
			"RTSP_PORT":     port,
			"MTX_CONN_TYPE": string(params.Desc.Type),
			"MTX_CONN_ID":   params.Desc.ID,
		}
	}

	if params.RunOnConnect != "" {
		params.Logger.Log(logger.Info, "runOnConnect command started")

		onConnectCmd = &externalcmd.Cmd{
			Pool:    params.ExternalCmdPool,
			Cmdstr:  params.RunOnConnect,
			Restart: params.RunOnConnectRestart,
			Env:     env,
			OnExit: func(err error) {
				params.Logger.Log(logger.Info, "runOnConnect command exited: %v", err)
			},
		}
		onConnectCmd.Start()
	}

	return func() {
		if onConnectCmd != nil {
			onConnectCmd.Close()
			params.Logger.Log(logger.Info, "runOnConnect command stopped")
		}

		if params.RunOnDisconnect != "" {
			params.Logger.Log(logger.Info, "runOnDisconnect command launched")
			cmd := &externalcmd.Cmd{
				Pool:    params.ExternalCmdPool,
				Cmdstr:  params.RunOnDisconnect,
				Restart: false,
				Env:     env,
			}
			cmd.Start()
		}
	}
}
