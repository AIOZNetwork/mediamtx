package api //nolint:revive

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/bluenviron/mediamtx/internal/models"
)

type apiLiveSegmentReport struct {
	StreamID       string    `json:"stream_id"`
	BaseStreamID   string    `json:"base_stream_id"`
	Rendition      string    `json:"rendition"`
	TrackType      string    `json:"track_type"`
	SegmentName    string    `json:"segment_name"`
	MuxSessionID   string    `json:"mux_session_id"`
	StorageBackend string    `json:"storage_backend"`
	StorageKey     string    `json:"storage_key"`
	StartedAt      time.Time `json:"started_at"`
	DurationMS     int64     `json:"duration_ms"`
	SizeBytes      int64     `json:"size_bytes"`
	Sequence       *int64    `json:"sequence"`
}

func (a *API) onInternalLiveSegments(ctx *gin.Context) {
	if a.LiveSegments == nil {
		a.writeError(ctx, http.StatusServiceUnavailable, fmt.Errorf("live segment repository unavailable"))
		return
	}

	var req apiLiveSegmentReport
	if err := ctx.ShouldBindJSON(&req); err != nil {
		a.writeError(ctx, http.StatusBadRequest, err)
		return
	}

	switch {
	case strings.TrimSpace(req.StreamID) == "":
		a.writeError(ctx, http.StatusBadRequest, fmt.Errorf("stream_id is required"))
		return
	case strings.TrimSpace(req.SegmentName) == "":
		a.writeError(ctx, http.StatusBadRequest, fmt.Errorf("segment_name is required"))
		return
	case strings.TrimSpace(req.Rendition) == "":
		a.writeError(ctx, http.StatusBadRequest, fmt.Errorf("rendition is required"))
		return
	case req.DurationMS <= 0:
		a.writeError(ctx, http.StatusBadRequest, fmt.Errorf("duration_ms must be greater than 0"))
		return
	case req.StartedAt.IsZero():
		a.writeError(ctx, http.StatusBadRequest, fmt.Errorf("started_at is required"))
		return
	}

	now := time.Now()
	segment := models.LiveHLSSegment{
		StreamID:       req.StreamID,
		BaseStreamID:   req.BaseStreamID,
		MuxSessionID:   req.MuxSessionID,
		TrackType:      req.TrackType,
		Rendition:      req.Rendition,
		SegmentName:    req.SegmentName,
		StorageBackend: req.StorageBackend,
		StorageKey:     req.StorageKey,
		StartedAt:      req.StartedAt,
		DurationMS:     req.DurationMS,
		SizeBytes:      req.SizeBytes,
		Sequence:       req.Sequence,
		UploadedAt:     &now,
	}

	if err := a.LiveSegments.UpsertUploaded(segment); err != nil {
		a.writeError(ctx, http.StatusInternalServerError, err)
		return
	}

	ctx.Status(http.StatusNoContent)
}
