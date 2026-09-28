package rtmp

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/google/uuid"

	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/models"
	"github.com/bluenviron/mediamtx/internal/stream"
	"github.com/bluenviron/mediamtx/internal/unit"
)

// publishStatistics writes the incoming bitrate and frame rate of a published
// stream to live_stream_statistics every conf.SecondCalculate seconds, keyed
// by the media id the stream is published under. aioz-stream's live
// statistics endpoint reads them.
type publishStatistics struct {
	mediaID       uuid.UUID
	bytesReceived func() uint64
	repo          models.LiveStreamStatisticRepository
	parent        logger.Writer

	frames atomic.Uint64
}

// run counts the video frames of strm and writes the statistics until ctx is
// done.
func (ps *publishStatistics) run(
	ctx context.Context,
	strm *stream.Stream,
	medias []*description.Media,
) {
	r := &stream.Reader{
		SkipOutboundBytes: true,
		Parent:            ps.parent,
	}

	for _, medi := range medias {
		if medi.Type != description.MediaTypeVideo {
			continue
		}
		for _, forma := range medi.Formats {
			r.OnData(medi, forma, func(u *unit.Unit) error {
				if !u.NilPayload() {
					ps.frames.Add(1)
				}
				return nil
			})
		}
	}

	if r.Formats() != nil {
		strm.AddReader(r)
		defer strm.RemoveReader(r)
	}

	interval := time.Duration(conf.SecondCalculate) * time.Second
	if interval <= 0 {
		interval = 5 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	lastTime := time.Now()
	lastBytes := ps.bytesReceived()

	for {
		select {
		case <-ctx.Done():
			return

		case now := <-ticker.C:
			elapsed := now.Sub(lastTime).Seconds()
			bytes := ps.bytesReceived()
			frames := ps.frames.Swap(0)

			bitrate := float64(bytes-lastBytes) * 8 / elapsed
			fps := int16(math.Round(float64(frames) / elapsed))

			lastTime = now
			lastBytes = bytes

			err := ps.repo.UpsertBitrateIn(ps.mediaID, bitrate)
			if err != nil {
				ps.parent.Log(logger.Warn, "unable to save bitrate: %v", err)
			}

			err = ps.repo.UpsertFPSIn(ps.mediaID, fps)
			if err != nil {
				ps.parent.Log(logger.Warn, "unable to save frame rate: %v", err)
			}
		}
	}
}
