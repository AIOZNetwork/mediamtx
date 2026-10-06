package api

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/bluenviron/mediamtx/internal/database/repository"
	"github.com/bluenviron/mediamtx/internal/models"
	"github.com/gin-gonic/gin"
)

type providerHLSMetadataIngestRequest struct {
	EventID      string                       `json:"event_id"`
	StreamID     string                       `json:"stream_id"`
	MediaMTXPath string                       `json:"mediamtx_path"`
	JobID        string                       `json:"job_id"`
	Generation   int64                        `json:"generation"`
	Rendition    providerHLSMetadataRendition `json:"rendition"`
	Object       providerHLSMetadataObject    `json:"object"`
	HLS          providerHLSMetadataHLS       `json:"hls"`
	PublishedAt  time.Time                    `json:"published_at"`
}

type providerHLSMetadataRendition struct {
	Name      string `json:"name"`
	Width     int    `json:"width"`
	Height    int    `json:"height"`
	Bandwidth int64  `json:"bandwidth"`
	Codecs    string `json:"codecs"`
}

type providerHLSMetadataObject struct {
	Kind        string `json:"kind"`
	Name        string `json:"name"`
	Provider    string `json:"provider"`
	ContractID  string `json:"contract_id"`
	StorageKey  string `json:"storage_key"`
	URL         string `json:"url"`
	ObjectID    string `json:"object_id"`
	ContentType string `json:"content_type"`
	SizeBytes   int64  `json:"size_bytes"`
}

type providerHLSMetadataHLS struct {
	MediaSequence   *int64     `json:"media_sequence"`
	DurationMS      int64      `json:"duration_ms"`
	ProgramDateTime *time.Time `json:"program_date_time"`
	InitSegmentName string     `json:"init_segment_name"`
	InitStorageKey  string     `json:"init_storage_key"`
}

func (req providerHLSMetadataIngestRequest) toModel(idempotencyKey string) models.ProviderHLSMetadata {
	return models.ProviderHLSMetadata{
		EventID:         req.EventID,
		IdempotencyKey:  idempotencyKey,
		StreamID:        req.StreamID,
		MediaMTXPath:    req.MediaMTXPath,
		JobID:           req.JobID,
		Generation:      req.Generation,
		RenditionName:   req.Rendition.Name,
		Width:           req.Rendition.Width,
		Height:          req.Rendition.Height,
		Bandwidth:       req.Rendition.Bandwidth,
		Codecs:          req.Rendition.Codecs,
		Kind:            req.Object.Kind,
		ObjectName:      req.Object.Name,
		Provider:        req.Object.Provider,
		ContractID:      req.Object.ContractID,
		StorageKey:      req.Object.StorageKey,
		URL:             req.Object.URL,
		ObjectID:        req.Object.ObjectID,
		ContentType:     req.Object.ContentType,
		SizeBytes:       req.Object.SizeBytes,
		MediaSequence:   req.HLS.MediaSequence,
		DurationMS:      req.HLS.DurationMS,
		ProgramDateTime: req.HLS.ProgramDateTime,
		InitSegmentName: req.HLS.InitSegmentName,
		InitStorageKey:  req.HLS.InitStorageKey,
		PublishedAt:     req.PublishedAt,
	}
}

func (a *API) onProviderHLSMetadataIngest(ctx *gin.Context) {
	if a.ProviderHLSMetadataRepository == nil {
		a.writeError(ctx, http.StatusServiceUnavailable, fmt.Errorf("provider HLS metadata repository is not configured"))
		return
	}

	idempotencyKey := ctx.GetHeader("Idempotency-Key")
	if idempotencyKey == "" {
		a.writeError(ctx, http.StatusBadRequest, fmt.Errorf("Idempotency-Key header is required"))
		return
	}

	var req providerHLSMetadataIngestRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		a.writeError(ctx, http.StatusBadRequest, err)
		return
	}

	stored, err := a.ProviderHLSMetadataRepository.UpsertByIdempotencyKey(req.toModel(idempotencyKey))
	if err != nil {
		if errors.Is(err, repository.ErrProviderHLSIdempotencyConflict) {
			a.writeError(ctx, http.StatusConflict, err)
			return
		}
		a.writeError(ctx, http.StatusBadRequest, err)
		return
	}

	ctx.JSON(http.StatusOK, stored)
}
