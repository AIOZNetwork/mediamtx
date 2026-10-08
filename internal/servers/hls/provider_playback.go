package hls

import (
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/bluenviron/mediamtx/internal/models"
)

// ProviderPlaybackService renders provider-backed fMP4 HLS from persisted metadata only.
// It intentionally has no muxer or filesystem dependency, so provider-required playback
// cannot fall back to local HLS state.
type ProviderPlaybackService struct {
	Repository models.ProviderHLSMetadataRepository
}

type ProviderPlaylistRequest struct {
	StreamID      string
	RenditionName string
	Generation    int64
	BasePath      string
	OriginalQuery string
}

type ProviderObjectRequest struct {
	StreamID      string
	RenditionName string
	Generation    int64
	ObjectName    string
}

const providerPlaylistSegmentCount = 7

func (s *ProviderPlaybackService) RenderMasterPlaylist(req ProviderPlaylistRequest) ([]byte, error) {
	items, err := s.list(req.StreamID, "", 0)
	if err != nil {
		return nil, err
	}
	latestGeneration := latestProviderGeneration(items)
	if latestGeneration == 0 {
		return nil, fmt.Errorf("provider HLS metadata not found")
	}
	renditions := providerRenditionsForGeneration(items, latestGeneration)
	if len(renditions) == 0 {
		return nil, fmt.Errorf("provider HLS metadata not found")
	}

	var b strings.Builder
	b.WriteString("#EXTM3U\n")
	b.WriteString("#EXT-X-VERSION:7\n")
	b.WriteString("#EXT-X-INDEPENDENT-SEGMENTS\n")
	for _, r := range renditions {
		b.WriteString(fmt.Sprintf(
			"#EXT-X-STREAM-INF:BANDWIDTH=%d,AVERAGE-BANDWIDTH=%d,RESOLUTION=%dx%d,CODECS=\"%s\",CLOSED-CAPTIONS=NONE\n",
			r.Bandwidth, r.Bandwidth, r.Width, r.Height, r.Codecs,
		))
		b.WriteString(escapePathSegment(r.Name))
		b.WriteString(fmt.Sprintf("/index.m3u8?generation=%d", latestGeneration))
		b.WriteString("\n")
	}
	return []byte(b.String()), nil
}

func (s *ProviderPlaybackService) RenderHybridMasterPlaylist(req ProviderPlaylistRequest) ([]byte, error) {
	items, err := s.list(req.StreamID, "", 0)
	if err != nil {
		return nil, err
	}
	latestGeneration := latestProviderGeneration(items)
	renditions := providerRenditionsForGeneration(items, latestGeneration)
	if len(renditions) == 0 {
		return renderABRMasterPlaylist(nil, defaultMasterCodecString), nil
	}

	var b strings.Builder
	b.WriteString("#EXTM3U\n")
	b.WriteString("#EXT-X-VERSION:7\n")
	b.WriteString("#EXT-X-INDEPENDENT-SEGMENTS\n")
	for _, r := range renditions {
		b.WriteString(fmt.Sprintf(
			"#EXT-X-STREAM-INF:BANDWIDTH=%d,AVERAGE-BANDWIDTH=%d,RESOLUTION=%dx%d,CODECS=\"%s\",CLOSED-CAPTIONS=NONE\n",
			r.Bandwidth, r.Bandwidth, r.Width, r.Height, r.Codecs,
		))
		b.WriteString(escapePathSegment(r.Name))
		b.WriteString(fmt.Sprintf("/index.m3u8?generation=%d", latestGeneration))
		b.WriteString("\n")
	}
	return []byte(b.String()), nil
}

func (s *ProviderPlaybackService) RenderMediaPlaylist(req ProviderPlaylistRequest) ([]byte, error) {
	if req.RenditionName == "" {
		return nil, fmt.Errorf("rendition_name is required")
	}
	items, err := s.list(req.StreamID, req.RenditionName, 0)
	if err != nil {
		return nil, err
	}
	inits := make(map[string]models.ProviderHLSMetadata)
	segments := make([]models.ProviderHLSMetadata, 0)
	for i := range items {
		item := items[i]
		switch item.Kind {
		case models.ProviderHLSObjectKindInit:
			inits[providerInitKey(item.Generation, item.ObjectName, item.StorageKey)] = item
		case models.ProviderHLSObjectKindSegment:
			segments = append(segments, item)
		}
	}
	if len(inits) == 0 || len(segments) == 0 {
		return nil, fmt.Errorf("provider HLS init and segment metadata not found")
	}
	sort.SliceStable(segments, func(i, j int) bool {
		return *segments[i].MediaSequence < *segments[j].MediaSequence
	})
	// A sequence is monotonic across encoder generations. Keep a compact live
	// window while retaining enough history to derive the discontinuity sequence.
	windowStart := 0
	if len(segments) > providerPlaylistSegmentCount {
		windowStart = len(segments) - providerPlaylistSegmentCount
	}
	discontinuitySequence := int64(0)
	for i := 1; i <= windowStart; i++ {
		if segments[i].Generation != segments[i-1].Generation {
			discontinuitySequence++
		}
	}
	segments = segments[windowStart:]

	targetDuration := 1
	for _, segment := range segments {
		dur := int(math.Ceil(float64(segment.DurationMS) / 1000.0))
		if dur > targetDuration {
			targetDuration = dur
		}
	}
	mediaSequence := int64(0)
	if segments[0].MediaSequence != nil {
		mediaSequence = *segments[0].MediaSequence
	}

	var b strings.Builder
	b.WriteString("#EXTM3U\n")
	b.WriteString("#EXT-X-VERSION:7\n")
	b.WriteString("#EXT-X-INDEPENDENT-SEGMENTS\n")
	b.WriteString(fmt.Sprintf("#EXT-X-TARGETDURATION:%d\n", targetDuration))
	b.WriteString(fmt.Sprintf("#EXT-X-MEDIA-SEQUENCE:%d\n", mediaSequence))
	b.WriteString(fmt.Sprintf("#EXT-X-DISCONTINUITY-SEQUENCE:%d\n", discontinuitySequence))
	var previousGeneration int64
	for i, segment := range segments {
		if i == 0 || segment.Generation != previousGeneration {
			if i > 0 {
				b.WriteString("#EXT-X-DISCONTINUITY\n")
			}
			init, ok := inits[providerInitKey(segment.Generation, segment.InitSegmentName, segment.InitStorageKey)]
			if !ok {
				return nil, fmt.Errorf("provider HLS init metadata not found for generation %d", segment.Generation)
			}
			b.WriteString(fmt.Sprintf("#EXT-X-MAP:URI=\"%s\"\n", providerMediaURL(req.StreamID, req.RenditionName, segment.Generation, init.ObjectName)))
			previousGeneration = segment.Generation
		}
		if segment.ProgramDateTime != nil {
			b.WriteString("#EXT-X-PROGRAM-DATE-TIME:")
			b.WriteString(segment.ProgramDateTime.UTC().Format("2006-01-02T15:04:05.000Z"))
			b.WriteString("\n")
		}
		b.WriteString(fmt.Sprintf("#EXTINF:%.3f,\n", float64(segment.DurationMS)/1000.0))
		b.WriteString(providerMediaURL(req.StreamID, req.RenditionName, segment.Generation, segment.ObjectName))
		b.WriteString("\n")
	}
	return []byte(b.String()), nil
}

func (s *ProviderPlaybackService) ResolveObject(req ProviderObjectRequest) (*models.ProviderHLSMetadata, error) {
	items, err := s.list(req.StreamID, req.RenditionName, req.Generation)
	if err != nil {
		return nil, err
	}
	for i := range items {
		if items[i].ObjectName == req.ObjectName {
			if items[i].URL == "" {
				return nil, fmt.Errorf("provider URL missing for object %s", req.ObjectName)
			}
			return &items[i], nil
		}
	}
	return nil, fmt.Errorf("provider HLS object not found")
}

func (s *ProviderPlaybackService) RedirectObject(w http.ResponseWriter, r *http.Request, req ProviderObjectRequest) error {
	obj, err := s.ResolveObject(req)
	if err != nil {
		return err
	}
	http.Redirect(w, r, obj.URL, http.StatusFound)
	return nil
}

func (s *ProviderPlaybackService) list(streamID string, renditionName string, generation int64) ([]models.ProviderHLSMetadata, error) {
	if s == nil || s.Repository == nil {
		return nil, fmt.Errorf("provider HLS metadata repository is not configured")
	}
	if streamID == "" {
		return nil, fmt.Errorf("stream_id is required")
	}
	return s.Repository.ListWindow(models.ProviderHLSMetadataWindowQuery{
		StreamID:      streamID,
		RenditionName: renditionName,
		Generation:    generation,
	})
}

type providerRendition struct {
	Name      string
	Width     int
	Height    int
	Bandwidth int64
	Codecs    string
}

func providerRenditions(items []models.ProviderHLSMetadata) []providerRendition {
	byName := make(map[string]providerRendition)
	for _, item := range items {
		if item.RenditionName == "" || item.Bandwidth <= 0 || item.Codecs == "" {
			continue
		}
		byName[item.RenditionName] = providerRendition{
			Name:      item.RenditionName,
			Width:     item.Width,
			Height:    item.Height,
			Bandwidth: item.Bandwidth,
			Codecs:    item.Codecs,
		}
	}
	out := make([]providerRendition, 0, len(byName))
	for _, r := range byName {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Bandwidth == out[j].Bandwidth {
			return out[i].Name < out[j].Name
		}
		return out[i].Bandwidth > out[j].Bandwidth
	})
	return out
}

func latestProviderGeneration(items []models.ProviderHLSMetadata) int64 {
	var latest int64
	for _, item := range items {
		if item.Generation > latest {
			latest = item.Generation
		}
	}
	return latest
}

func providerRenditionsForGeneration(items []models.ProviderHLSMetadata, generation int64) []providerRendition {
	filtered := make([]models.ProviderHLSMetadata, 0, len(items))
	for _, item := range items {
		if item.Generation == generation {
			filtered = append(filtered, item)
		}
	}
	return providerRenditions(filtered)
}

func providerInitKey(generation int64, objectName string, storageKey string) string {
	return fmt.Sprintf("%d\x00%s\x00%s", generation, objectName, storageKey)
}

func providerMediaURL(streamID string, renditionName string, generation int64, objectName string) string {
	return "/provider-hls/media/" + escapePathSegment(streamID) + "/" + escapePathSegment(renditionName) + "/" +
		fmt.Sprintf("%d", generation) + "/" + escapePathSegment(objectName)
}

func escapePathSegment(v string) string {
	return url.PathEscape(v)
}
