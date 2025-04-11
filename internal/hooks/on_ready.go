package hooks

import (
	"fmt"
	"strings"

	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/database/repository"
	"github.com/bluenviron/mediamtx/internal/defs"
	"github.com/bluenviron/mediamtx/internal/externalcmd"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/google/uuid"
)

var jsonData = map[string][]string{
	"mystream": {"rtmp://a.rtmp.youtube.com/live2/abcd"},
}

func ffmpegGenerator(sourceUrl string, forwardURIs []string) string {
	if len(forwardURIs) == 0 {
		return ""
	}

	input := fmt.Sprintf("ffmpeg -i %s", sourceUrl)

	outputs := make([]string, len(forwardURIs))

	for i, uri := range forwardURIs {
		switch {
		case strings.HasPrefix(uri, "rtmp://"):
			outputs[i] = fmt.Sprintf("-c copy -f flv %s", uri)
		case strings.HasPrefix(uri, "rtsp://"):
			outputs[i] = fmt.Sprintf("-c copy -f rtsp %s", uri)
		}
	}

	return fmt.Sprintf("%s %s", input, strings.Join(outputs, " "))
}

func getMultiStreams(key string) []string {
	uuid, err := uuid.Parse(key)
	if err != nil {
		return nil
	}

	repo := repository.NewLiveStreamMulticastRepository()
	data, err := repo.GetLiveStreamMulticastByStreamKey(uuid)

	if err != nil || data == nil {
		return nil
	}

	return data.LiveStreamMulticastUrls
}

// OnReadyParams are the parameters of OnReady.
type OnReadyParams struct {
	Logger          logger.Writer
	ExternalCmdPool *externalcmd.Pool
	Conf            *conf.Path
	ExternalCmdEnv  externalcmd.Environment
	Desc            defs.APIPathSourceOrReader
	Query           string
}

// OnReady is the OnReady hook.
func OnReady(params OnReadyParams) func() {
	var env externalcmd.Environment
	var onReadyCmd *externalcmd.Cmd
	var onMulticastCmd *externalcmd.Cmd

	if params.Conf.RunOnReady != "" || params.Conf.RunOnNotReady != "" || params.Conf.IsRunMulticast {
		env = params.ExternalCmdEnv
		env["MTX_QUERY"] = params.Query
		env["MTX_SOURCE_TYPE"] = params.Desc.Type
		env["MTX_SOURCE_ID"] = params.Desc.ID
	}

	if params.Conf.RunOnReady != "" {
		params.Logger.Log(logger.Info, "runOnReady command started")
		onReadyCmd = externalcmd.NewCmd(
			params.ExternalCmdPool,
			params.Conf.RunOnReady,
			params.Conf.RunOnReadyRestart,
			env,
			func(err error) {
				params.Logger.Log(logger.Info, "runOnReady command exited: %v", err)
			})
	}

	if params.Conf.IsRunMulticast {
		params.Logger.Log(logger.Info, "Run multicast command started")
		sourceUrl := fmt.Sprintf("rtmp://%s/%s", params.Conf.Hostname, env["MTX_PATH"])
		multiStreamsUrl := getMultiStreams(env["AIOZ_StreamKey"])

		ffmpegQuery := ffmpegGenerator(sourceUrl, multiStreamsUrl)

		if ffmpegQuery != "" {
			onReadyCmd = externalcmd.NewCmd(
			params.ExternalCmdPool,
			ffmpegQuery,
			params.Conf.RunOnReadyRestart,
			env,
			func(err error) {
				params.Logger.Log(logger.Info, "Run multicast command exited: %v", err)
			})
		}
	}

	return func() {
		if onReadyCmd != nil {
			onReadyCmd.Close()
			params.Logger.Log(logger.Info, "runOnReady command stopped")
		}

		if onMulticastCmd != nil {
			onMulticastCmd.Close()
			params.Logger.Log(logger.Info, "Run multicast command stopped")
		}

		if params.Conf.RunOnNotReady != "" {
			params.Logger.Log(logger.Info, "runOnNotReady command launched")
			externalcmd.NewCmd(
				params.ExternalCmdPool,
				params.Conf.RunOnNotReady,
				false,
				env,
				nil)
		}
	}
}
