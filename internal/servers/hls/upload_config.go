package hls

import (
	"github.com/bluenviron/mediamtx/internal/database"
	"github.com/bluenviron/mediamtx/internal/database/repository"
	"github.com/bluenviron/mediamtx/internal/hlss3uploader"
	"github.com/bluenviron/mediamtx/internal/logger"
)

// MuxerUploadConfig hides storage-provider details from muxer and muxerInstance.
// The HLS muxer only owns uploader lifecycle; it does not know whether files are
// persisted to S3, CDN, MinIO, local storage, or another backend.
type MuxerUploadConfig struct {
	Storage hlss3uploader.StorageConfig
}

func (c *MuxerUploadConfig) NewUploader(
	muxerDirectory string,
	streamName string,
	streamKey string,
	parent logger.Writer,
) *hlss3uploader.HLSS3Uploader {
	storageConfig := c.Storage
	storageConfig.Directory = muxerDirectory
	storageConfig.StreamName = streamName
	storageConfig.StreamKey = streamKey

	if storageConfig.Workers <= 0 {
		storageConfig.Workers = 4
	}

	return &hlss3uploader.HLSS3Uploader{
		Config:     storageConfig,
		Repository: repository.NewLiveHLSSegmentRepository(database.DB),
		Parent:     parent,
	}
}
