package hooks

import (
	"context"
	"io"
	"net"
	"os"
	"time"

	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/database"
	"github.com/bluenviron/mediamtx/internal/database/repository"
	"github.com/bluenviron/mediamtx/internal/defs"
	"github.com/bluenviron/mediamtx/internal/externalcmd"
	"github.com/bluenviron/mediamtx/internal/grpc_service"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/google/uuid"
)

// OnConnectParams are the parameters of OnConnect.
type OnConnectParams struct {
	Logger              logger.Writer
	ExternalCmdPool     *externalcmd.Pool
	RunOnConnect        string
	RunOnConnectRestart bool
	RunOnDisconnect     string
	RTSPAddress         string
	Desc                defs.APIPathSourceOrReader
}

// OnConnect is the OnConnect hook.
func OnConnect(params OnConnectParams) func() {
	var env externalcmd.Environment
	var onConnectCmd *externalcmd.Cmd

	if params.RunOnConnect != "" || params.RunOnDisconnect != "" {
		_, port, _ := net.SplitHostPort(params.RTSPAddress)
		env = externalcmd.Environment{
			"RTSP_PORT":       port,
			"MTX_CONN_TYPE":   params.Desc.Type,
			"MTX_CONN_ID":     params.Desc.ID,
			"WEBHOOK_ADDRESS": conf.WebhookAddress,
		}
	}

	if params.RunOnConnect != "" {
		params.Logger.Log(logger.Info, "runOnConnect command started")

		_, err := database.RedisIdDb.Set(context.Background(), params.Desc.ID, conf.IdentityServer, time.Duration(conf.RedisTTLHours)*time.Hour).Result()
		if err != nil {
			params.Logger.Log(logger.Error, "Failed to set connid in redis: %v", err)
		}

		_, err = database.RedisStatsDb.SAdd(context.Background(), conf.IdentityServer, params.Desc.ID).Result()
		if err != nil {
			params.Logger.Log(logger.Error, "Failed to set connid in redis for stats: %v", err)
		}

		onConnectCmd = externalcmd.NewCmd(
			params.ExternalCmdPool,
			params.RunOnConnect,
			params.RunOnConnectRestart,
			env,
			func(err error) {
				_, e := database.RedisIdDb.Del(context.Background(), params.Desc.ID).Result()
				if e != nil {
					params.Logger.Log(logger.Error, "Failed to remove connid in redis: %v", e)
				}

				params.Logger.Log(logger.Info, "runOnConnect command exited: %v", err)
			})
	}

	return func() {
		if onConnectCmd != nil {
			onConnectCmd.Close()
			_, err := database.RedisStatsDb.SRem(context.Background(), conf.IdentityServer, params.Desc.ID).Result()
			if err != nil {
				params.Logger.Log(logger.Error, "Failed to remove connid in redis for stats: %v", err)
			}
			params.Logger.Log(logger.Info, "runOnConnect command stopped")
		}

		if params.RunOnDisconnect != "" {
			_, err := database.RedisStatsDb.SRem(context.Background(), conf.IdentityServer, params.Desc.ID).Result()
			if err != nil {
				params.Logger.Log(logger.Error, "Failed to remove connid in redis for stats: %v", err)
			}

			params.Logger.Log(logger.Info, "runOnDisconnect command launched")
			externalcmd.NewCmd(
				params.ExternalCmdPool,
				params.RunOnDisconnect,
				false,
				env,
				nil)

			videoRepository := repository.NewLiveStreamVideoRepository(database.DB)
			connUuid, err := uuid.Parse(params.Desc.ID)
			if err != nil {
				params.Logger.Log(logger.Error, "Failed to parse connid: %v", err)
				return
			}

			video, err := videoRepository.GetStreamMediaByConnId(connUuid)
			if err != nil || video == nil {
				params.Logger.Log(logger.Error, "Failed to get stream video by connid: %v", err)
				return
			}

			videoId := video.Id.String()
			sourceDir := "./input/" + videoId + "/video.mp4"
			destDir := "./retry/" + videoId + "/video.mp4"
			videoData, err := os.Open(sourceDir)
			if err != nil {
				params.Logger.Log(logger.Error, "Failed to open video file: %v", err)
				return
			}
			defer videoData.Close()

			fi, err := videoData.Stat()
			if err != nil {
				params.Logger.Log(logger.Error, "Failed to get file info: %v", err)
				return
			}
			if fi.Size() < 1000000 { // Must more than 1MB
				params.Logger.Log(logger.Error, "File is too small")
				return
			}

			err = grpc_service.W3GrpcClient.UploadMedia(
				context.Background(),
				videoId,
				"video.mp4",
				fi.Size(),
				videoData,
			)

			if err != nil {
				params.Logger.Log(logger.Error, "Failed to upload media resource: %v", err)

				// Move file to retry folder to retry upload again
				retryDir := "./retry/" + videoId
				if err := os.MkdirAll(retryDir, 0755); err != nil {
					params.Logger.Log(logger.Error, "Cannot create media retry directory; Media ID: %s | %v", videoId, err)
					return
				}

				// Seek back to the beginning of the file before copying
				if _, err := videoData.Seek(0, 0); err != nil {
					params.Logger.Log(logger.Error, "Cannot seek video file; Media ID: %s | %v", videoId, err)
					return
				}

				destFile, err := os.Create(destDir)
				if err != nil {
					params.Logger.Log(logger.Error, "Cannot create media retry file; Media ID: %s | %v", videoId, err)
					return
				}
				defer destFile.Close()

				_, err = io.Copy(destFile, videoData)
				if err != nil {
					params.Logger.Log(logger.Error, "Cannot copy error upload Media ID: %s | %v", videoId, err)
					return
				}

				if err := destFile.Sync(); err != nil {
					params.Logger.Log(logger.Error, "Cannot sync copy error upload Media ID: %s | %v", videoId, err)
					return
				}
				return
			}
			params.Logger.Log(logger.Info, "Media resource uploaded successfully: %s", videoId)
		}
	}
}
