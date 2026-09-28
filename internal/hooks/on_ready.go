package hooks

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/database"
	"github.com/bluenviron/mediamtx/internal/database/repository"
	"github.com/bluenviron/mediamtx/internal/defs"
	"github.com/bluenviron/mediamtx/internal/externalcmd"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/google/uuid"
)

func ffmpegGenerator(sourceUrl string, forwardURIs []string) (string, error) {
	if len(forwardURIs) == 0 {
		return "", nil
	}

	input := fmt.Sprintf("ffmpeg -i %s", sourceUrl)

	outputs := make([]string, len(forwardURIs))

	for i, uri := range forwardURIs {
		parsedURL, err := url.Parse(uri)

		if err != nil || parsedURL.Scheme != "rtmp" {
			return "", err
		}
		switch {
		case strings.HasPrefix(parsedURL.String(), "rtmp://") && !strings.Contains(uri, " "):
			outputs[i] = fmt.Sprintf("-c copy -f flv %s", parsedURL.String())
		default:
			return "", nil
		}
	}

	return fmt.Sprintf("%s %s", input, strings.Join(outputs, " ")), nil
}

func getMultiStreams(key string) []string {
	uuid, err := uuid.Parse(key)
	if err != nil {
		return nil
	}

	repo := repository.NewLiveStreamMulticastRepository(database.DB)
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
	Desc            *defs.APIPathSource
	Query           string
}

// OnReady is the OnReady hook.
func OnReady(params OnReadyParams) func() {
	var env externalcmd.Environment
	var onReadyCmd *externalcmd.Cmd
	var onMulticastCmd *externalcmd.Cmd

	runOnReady := ""
	runOnReadyRestart := false
	runOnNotReady := ""
	if params.Conf.RunOnReady != nil {
		runOnReady = *params.Conf.RunOnReady
	}
	if params.Conf.RunOnReadyRestart != nil {
		runOnReadyRestart = *params.Conf.RunOnReadyRestart
	}
	if params.Conf.RunOnNotReady != nil {
		runOnNotReady = *params.Conf.RunOnNotReady
	}

	if runOnReady != "" || runOnNotReady != "" || params.Conf.IsRunMulticast {
		env = params.ExternalCmdEnv
		env["MTX_QUERY"] = params.Query
		if params.Desc != nil {
			env["MTX_SOURCE_TYPE"] = string(params.Desc.Type)
			env["MTX_SOURCE_ID"] = params.Desc.ID
		}
	}

	_, err := database.RedisIdDb.Set(context.Background(), env["MTX_PATH"], conf.IdentityServer, time.Duration(conf.RedisTTLHours)*time.Hour).Result()
	if err != nil {
		params.Logger.Log(logger.Error, "Failed to set connid in redis: %v", err)
	}

	if runOnReady != "" {
		params.Logger.Log(logger.Info, "runOnReady command started")
		onReadyCmd = &externalcmd.Cmd{
			Pool:    params.ExternalCmdPool,
			Cmdstr:  runOnReady,
			Restart: runOnReadyRestart,
			Env:     env,
			OnExit: func(err error) {
				params.Logger.Log(logger.Info, "runOnReady command exited: %v", err)
			},
		}
		onReadyCmd.Start()
	}

	if params.Conf.IsRunMulticast {
		params.Logger.Log(logger.Info, "Run multicast command started")
		sourceUrl := fmt.Sprintf("rtmp://%s/%s", params.Conf.Hostname, env["MTX_PATH"])
		multiStreamsUrl := getMultiStreams(env["AIOZ_StreamKey"])
		ffmpegQuery, err := ffmpegGenerator(sourceUrl, multiStreamsUrl)

		if err != nil {
			params.Logger.Log(logger.Error, "Error generating ffmpeg command: %v", err)
		}

		if ffmpegQuery != "" {
			onMulticastCmd = &externalcmd.Cmd{
				Pool:    params.ExternalCmdPool,
				Cmdstr:  ffmpegQuery,
				Restart: runOnReadyRestart,
				Env:     env,
				OnExit: func(err error) {
					params.Logger.Log(logger.Info, "Run multicast command exited: %v", err)
				},
			}
			onMulticastCmd.Start()
		}
	}

	return func() {
		if env["MTX_PATH"] != "" {
			_, err := database.RedisIdDb.Del(context.Background(), env["MTX_PATH"]).Result()
			if err != nil {
				params.Logger.Log(logger.Error, "Failed to remove path from redis: %v", err)
			}
		}

		if onReadyCmd != nil {
			onReadyCmd.Close()
			params.Logger.Log(logger.Info, "runOnReady command stopped")
		}

		if onMulticastCmd != nil {
			onMulticastCmd.Close()
			params.Logger.Log(logger.Info, "Run multicast command stopped")
		}

		if runOnNotReady != "" {
			params.Logger.Log(logger.Info, "runOnNotReady command launched")
			cmd := &externalcmd.Cmd{
				Pool:    params.ExternalCmdPool,
				Cmdstr:  runOnNotReady,
				Restart: false,
				Env:     env,
			}
			cmd.Start()
		}
	}
}
