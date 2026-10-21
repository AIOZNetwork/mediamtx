package models

import (
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

const (
	ProviderHLSObjectKindInit    = "init"
	ProviderHLSObjectKindSegment = "segment"
)

type ProviderHLSMetadata struct {
	ID              uuid.UUID      `json:"id" gorm:"primaryKey;type:uuid"`
	EventID         string         `json:"event_id" gorm:"index"`
	IdempotencyKey  string         `json:"idempotency_key" gorm:"uniqueIndex;not null"`
	StreamID        string         `json:"stream_id" gorm:"index:idx_provider_hls_stream_rendition_generation;uniqueIndex:idx_provider_hls_object_identity;uniqueIndex:idx_provider_hls_sequence_identity"`
	MediaMTXPath    string         `json:"mediamtx_path" gorm:"column:mediamtx_path;index"`
	JobID           string         `json:"job_id" gorm:"index"`
	Generation      int64          `json:"generation" gorm:"index:idx_provider_hls_stream_rendition_generation;uniqueIndex:idx_provider_hls_object_identity"`
	RenditionName   string         `json:"rendition_name" gorm:"index:idx_provider_hls_stream_rendition_generation;uniqueIndex:idx_provider_hls_object_identity;uniqueIndex:idx_provider_hls_sequence_identity"`
	Width           int            `json:"width"`
	Height          int            `json:"height"`
	Bandwidth       int64          `json:"bandwidth"`
	Codecs          string         `json:"codecs"`
	Kind            string         `json:"kind" gorm:"index;uniqueIndex:idx_provider_hls_object_identity"`
	ObjectName      string         `json:"object_name" gorm:"index;uniqueIndex:idx_provider_hls_object_identity"`
	Provider        string         `json:"provider" gorm:"index"`
	ContractID      string         `json:"contract_id" gorm:"index"`
	StorageKey      string         `json:"storage_key" gorm:"index"`
	URL             string         `json:"url"`
	ObjectID        string         `json:"object_id" gorm:"index"`
	ContentType     string         `json:"content_type"`
	SizeBytes       int64          `json:"size_bytes"`
	MediaSequence   *int64         `json:"media_sequence" gorm:"index;uniqueIndex:idx_provider_hls_sequence_identity"`
	DurationMS      int64          `json:"duration_ms"`
	ProgramDateTime *time.Time     `json:"program_date_time"`
	InitSegmentName string         `json:"init_segment_name"`
	InitStorageKey  string         `json:"init_storage_key"`
	PublishedAt     time.Time      `json:"published_at" gorm:"index"`
	CreatedAt       time.Time      `json:"created_at"`
	UpdatedAt       time.Time      `json:"updated_at"`
	DeletedAt       gorm.DeletedAt `json:"-" gorm:"index"`
}

func (ProviderHLSMetadata) TableName() string {
	return "provider_hls_metadata"
}

func (m ProviderHLSMetadata) Validate() error {
	if m.IdempotencyKey == "" {
		return fmt.Errorf("idempotency_key is required")
	}
	if m.StreamID == "" {
		return fmt.Errorf("stream_id is required")
	}
	if m.MediaMTXPath == "" {
		return fmt.Errorf("mediamtx_path is required")
	}
	if m.Generation <= 0 {
		return fmt.Errorf("generation is required")
	}
	if m.RenditionName == "" {
		return fmt.Errorf("rendition_name is required")
	}
	if m.Bandwidth <= 0 {
		return fmt.Errorf("rendition bandwidth is required")
	}
	if m.Codecs == "" {
		return fmt.Errorf("rendition codecs is required")
	}
	if m.Kind != ProviderHLSObjectKindInit && m.Kind != ProviderHLSObjectKindSegment {
		return fmt.Errorf("kind must be %q or %q", ProviderHLSObjectKindInit, ProviderHLSObjectKindSegment)
	}
	if m.ObjectName == "" {
		return fmt.Errorf("object_name is required")
	}
	if m.Provider == "" {
		return fmt.Errorf("provider is required")
	}
	if m.StorageKey == "" {
		return fmt.Errorf("storage_key is required")
	}
	if m.ContentType == "" {
		return fmt.Errorf("content_type is required")
	}
	if m.PublishedAt.IsZero() {
		return fmt.Errorf("published_at is required")
	}
	if m.Kind == ProviderHLSObjectKindSegment {
		if m.MediaSequence == nil {
			return fmt.Errorf("media_sequence is required for segment objects")
		}
		if m.DurationMS <= 0 {
			return fmt.Errorf("duration_ms must be greater than zero for segment objects")
		}
		if m.InitSegmentName == "" {
			return fmt.Errorf("init_segment_name is required for segment objects")
		}
		if m.InitStorageKey == "" {
			return fmt.Errorf("init_storage_key is required for segment objects")
		}
	}
	return nil
}

func NormalizeProviderHLSTime(t time.Time) time.Time {
	if t.IsZero() {
		return t
	}
	return t.UTC().Truncate(time.Microsecond)
}

func NormalizeProviderHLSTimePtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	out := NormalizeProviderHLSTime(*t)
	return &out
}

func (m *ProviderHLSMetadata) NormalizeForStorage() {
	m.PublishedAt = NormalizeProviderHLSTime(m.PublishedAt)
	m.ProgramDateTime = NormalizeProviderHLSTimePtr(m.ProgramDateTime)
}

func (m ProviderHLSMetadata) HasSameCanonicalPayload(other ProviderHLSMetadata) bool {
	m.NormalizeForStorage()
	other.NormalizeForStorage()
	return m.EventID == other.EventID &&
		m.StreamID == other.StreamID &&
		m.MediaMTXPath == other.MediaMTXPath &&
		m.JobID == other.JobID &&
		m.Generation == other.Generation &&
		m.RenditionName == other.RenditionName &&
		m.Width == other.Width &&
		m.Height == other.Height &&
		m.Bandwidth == other.Bandwidth &&
		m.Codecs == other.Codecs &&
		m.Kind == other.Kind &&
		m.ObjectName == other.ObjectName &&
		m.Provider == other.Provider &&
		m.ContractID == other.ContractID &&
		m.StorageKey == other.StorageKey &&
		m.URL == other.URL &&
		m.ObjectID == other.ObjectID &&
		m.ContentType == other.ContentType &&
		m.SizeBytes == other.SizeBytes &&
		reflect.DeepEqual(m.MediaSequence, other.MediaSequence) &&
		m.DurationMS == other.DurationMS &&
		equalTimes(m.ProgramDateTime, other.ProgramDateTime) &&
		m.InitSegmentName == other.InitSegmentName &&
		m.InitStorageKey == other.InitStorageKey &&
		m.PublishedAt.Equal(other.PublishedAt)
}

func equalTimes(a *time.Time, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

type ProviderHLSMetadataWindowQuery struct {
	StreamID          string
	RenditionName     string
	Generation        int64
	FromMediaSequence *int64
	ToMediaSequence   *int64
	SincePublishedAt  *time.Time
	Limit             int
}

type ProviderHLSMetadataRepository interface {
	UpsertByIdempotencyKey(metadata ProviderHLSMetadata) (*ProviderHLSMetadata, error)
	DeleteByStreamID(streamID string) (int64, error)
	ListWindow(query ProviderHLSMetadataWindowQuery) ([]ProviderHLSMetadata, error)
}
