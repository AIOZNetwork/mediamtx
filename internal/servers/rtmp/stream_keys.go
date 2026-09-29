package rtmp

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	"github.com/bluenviron/mediamtx/internal/database/repository"
	"github.com/bluenviron/mediamtx/internal/models"
)

// liveStreamMediaStatusEnd is aioz-stream's live_stream_media.status of a
// finished session (domain.LiveStreamStatusEnd).
const liveStreamMediaStatusEnd = "end"

var (
	errStreamKeyPublishing = errors.New("this streamkey is streaming")
	errStreamKeyInvalid    = errors.New("invalid path name")
)

// PublishTarget is where a publisher's stream key sends its stream.
type PublishTarget struct {
	// PathName is the id of the media session the stream publishes under.
	PathName string
	// StreamKey is the key the publisher connected with.
	StreamKey string
	// KeyType is live_stream_keys.type: "audio" publishes audio only.
	KeyType string
}

func (t *PublishTarget) streamKey() string {
	if t == nil {
		return ""
	}
	return t.StreamKey
}

// StreamKeys resolves the AIOZ stream key a publisher connects with.
//
// A publish URL does not name a path: its last segment is a stream key, and
// the stream is published under the id of the key's media session. Without
// StreamKeys the server publishes to the URL path as upstream mediamtx does.
type StreamKeys interface {
	// ResolvePublish maps a publish URL to its PublishTarget. isPublishing
	// reports whether a key is being published through this server.
	ResolvePublish(
		ctx context.Context,
		u *url.URL,
		isPublishing func(streamKey string) bool,
	) (*PublishTarget, error)

	// Statistics stores the statistics of published streams.
	Statistics() models.LiveStreamStatisticRepository
}

// AIOZStreamKeys resolves stream keys against aioz-stream's live_stream_keys
// and live_stream_media tables, and the Redis record of which server
// publishes each media session.
type AIOZStreamKeys struct {
	videos         *repository.LiveStreamVideoRepository
	keys           models.LiveStreamKeyRepository
	statistics     models.LiveStreamStatisticRepository
	redis          *redis.Client
	identityServer string
}

// NewAIOZStreamKeys returns the AIOZStreamKeys of a database. rdb may be nil
// when Redis is not configured; the cross-server check is then skipped.
func NewAIOZStreamKeys(
	db *gorm.DB,
	rdb *redis.Client,
	identityServer string,
) *AIOZStreamKeys {
	return &AIOZStreamKeys{
		videos:         repository.NewLiveStreamVideoRepository(db),
		keys:           repository.NewLiveStreamKeyRepository(db),
		statistics:     repository.NewLiveStreamStatisticsRepository(db),
		redis:          rdb,
		identityServer: identityServer,
	}
}

// Statistics implements StreamKeys.
func (k *AIOZStreamKeys) Statistics() models.LiveStreamStatisticRepository {
	return k.statistics
}

// ResolvePublish implements StreamKeys.
func (k *AIOZStreamKeys) ResolvePublish(
	ctx context.Context,
	u *url.URL,
	isPublishing func(streamKey string) bool,
) (*PublishTarget, error) {
	pathName := strings.Trim(u.Path, "/")

	streamKey := pathName
	if idx := strings.LastIndex(pathName, "/"); idx != -1 {
		streamKey = pathName[idx+1:]
	}

	if isPublishing(streamKey) {
		return nil, errStreamKeyPublishing
	}

	streamKeyID, err := uuid.Parse(streamKey)
	if err != nil {
		return nil, errStreamKeyInvalid
	}

	liveStreamKey, err := k.keys.GetLiveStreamKeyByStreamKey(streamKeyID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, errStreamKeyInvalid
	}
	if err != nil {
		return nil, fmt.Errorf("unable to read stream key: %w", err)
	}

	target := &PublishTarget{
		StreamKey: streamKey,
		KeyType:   liveStreamKey.Type,
	}

	media, err := k.videos.GetStreamMediaAvaialbleByStreamKey(streamKeyID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		// Streaming directly, without a session created through the API.
		target.PathName = uuid.NewString()
		return target, nil
	}
	if err != nil {
		return nil, fmt.Errorf("unable to read media session: %w", err)
	}

	if media.Status == "streaming" && k.redis != nil {
		server, _ := k.redis.Get(ctx, media.Id.String()).Result()
		if server != "" {
			// Redis names this server, but this server is not publishing the
			// key: the previous session here crashed or ended without a clean
			// disconnect, so it is closed and a new one starts.
			if server == k.identityServer && !isPublishing(streamKey) {
				_ = k.redis.Del(ctx, media.Id.String()).Err()
				_ = k.videos.UpdateStreamMediaStatus(media.Id, liveStreamMediaStatusEnd)

				target.PathName = uuid.NewString()
				return target, nil
			}

			return nil, errStreamKeyPublishing
		}
	}

	target.PathName = media.Id.String()
	return target, nil
}

var _ StreamKeys = (*AIOZStreamKeys)(nil)
