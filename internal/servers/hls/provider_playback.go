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

func (s *ProviderPlaybackService) RenderMasterPlaylist(req ProviderPlaylistRequest) ([]byte, error) {
	items, err := s.list(req.StreamID, "", req.Generation)
	if err != nil {
		return nil, err
	}
	renditions := providerRenditions(items)
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
		b.WriteString("/index.m3u8?generation=")
		b.WriteString(fmt.Sprintf("%d", req.Generation))
		b.WriteString("\n")
	}
	return []byte(b.String()), nil
}

func (s *ProviderPlaybackService) RenderHybridMasterPlaylist(req ProviderPlaylistRequest) ([]byte, error) {
	items, err := s.list(req.StreamID, "", req.Generation)
	if err != nil {
		return nil, err
	}
	renditions := providerRenditions(items)
	if len(renditions) == 0 {
		return nil, fmt.Errorf("provider HLS metadata not found")
	}

	originalBandwidth := sourceBandwidth
	topWidth := 0
	topHeight := 0
	codecStr := defaultMasterCodecString
	for _, r := range renditions {
		if int(r.Bandwidth) > originalBandwidth {
			originalBandwidth = int(r.Bandwidth) * 120 / 100
		}
		if r.Width > topWidth {
			topWidth = r.Width
			topHeight = r.Height
			codecStr = r.Codecs
		}
	}
	resolutionAttr := ""
	if topWidth > 0 && topHeight > 0 {
		resolutionAttr = fmt.Sprintf("RESOLUTION=%dx%d,", topWidth, topHeight)
	}
	originalURI := "original/index.m3u8"
	if req.OriginalQuery != "" {
		originalURI += "?" + req.OriginalQuery
	}

	var b strings.Builder
	b.WriteString("#EXTM3U\n")
	b.WriteString("#EXT-X-VERSION:7\n")
	b.WriteString("#EXT-X-INDEPENDENT-SEGMENTS\n")
	b.WriteString(fmt.Sprintf(
		"#EXT-X-STREAM-INF:BANDWIDTH=%d,AVERAGE-BANDWIDTH=%d,%sCODECS=\"%s\",CLOSED-CAPTIONS=NONE\n",
		originalBandwidth, originalBandwidth, resolutionAttr, codecStr,
	))
	b.WriteString(originalURI)
	b.WriteString("\n")
	for _, r := range renditions {
		b.WriteString(fmt.Sprintf(
			"#EXT-X-STREAM-INF:BANDWIDTH=%d,AVERAGE-BANDWIDTH=%d,RESOLUTION=%dx%d,CODECS=\"%s\",CLOSED-CAPTIONS=NONE\n",
			r.Bandwidth, r.Bandwidth, r.Width, r.Height, r.Codecs,
		))
		b.WriteString(escapePathSegment(r.Name))
		b.WriteString("/index.m3u8?generation=")
		b.WriteString(fmt.Sprintf("%d", req.Generation))
		b.WriteString("\n")
	}
	return []byte(b.String()), nil
}

func (s *ProviderPlaybackService) RenderMediaPlaylist(req ProviderPlaylistRequest) ([]byte, error) {
	if req.RenditionName == "" {
		return nil, fmt.Errorf("rendition_name is required")
	}
	items, err := s.list(req.StreamID, req.RenditionName, req.Generation)
	if err != nil {
		return nil, err
	}
	var init *models.ProviderHLSMetadata
	segments := make([]models.ProviderHLSMetadata, 0)
	for i := range items {
		item := items[i]
		switch item.Kind {
		case models.ProviderHLSObjectKindInit:
			if init == nil {
				init = &item
			}
		case models.ProviderHLSObjectKindSegment:
			segments = append(segments, item)
		}
	}
	if init == nil || len(segments) == 0 {
		return nil, fmt.Errorf("provider HLS init and segment metadata not found")
	}
	sort.SliceStable(segments, func(i, j int) bool {
		return *segments[i].MediaSequence < *segments[j].MediaSequence
	})

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
	b.WriteString(fmt.Sprintf("#EXT-X-MAP:URI=\"%s\"\n", providerMediaURL(req.StreamID, req.RenditionName, req.Generation, init.ObjectName)))
	for _, segment := range segments {
		if segment.ProgramDateTime != nil {
			b.WriteString("#EXT-X-PROGRAM-DATE-TIME:")
			b.WriteString(segment.ProgramDateTime.UTC().Format("2006-01-02T15:04:05.000Z"))
			b.WriteString("\n")
		}
		b.WriteString(fmt.Sprintf("#EXTINF:%.3f,\n", float64(segment.DurationMS)/1000.0))
		b.WriteString(providerMediaURL(req.StreamID, req.RenditionName, req.Generation, segment.ObjectName))
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
	if generation == 0 {
		return nil, fmt.Errorf("generation is required")
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

func providerMediaURL(streamID string, renditionName string, generation int64, objectName string) string {
	return "/provider-hls/media/" + escapePathSegment(streamID) + "/" + escapePathSegment(renditionName) + "/" +
		fmt.Sprintf("%d", generation) + "/" + escapePathSegment(objectName)
}

func escapePathSegment(v string) string {
	return url.PathEscape(v)
}
