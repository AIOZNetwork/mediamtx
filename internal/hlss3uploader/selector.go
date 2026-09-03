package hlss3uploader

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
)

// ProviderType represents a strongly-typed storage provider identifier.
type ProviderType string

const (
	ProviderS3    ProviderType = "s3"
	ProviderDePIN ProviderType = "depin"
)

// StorageConfig contains settings to configure storage providers.
type StorageConfig struct {
	Provider               string `json:"provider" yaml:"provider"`
	Directory              string `json:"directory" yaml:"directory"`
	StreamName             string `json:"stream_name" yaml:"stream_name"`
	StreamKey              string `json:"stream_key" yaml:"stream_key"`
	Prefix                 string `json:"prefix" yaml:"prefix"`
	Endpoint               string `json:"endpoint" yaml:"endpoint"`
	Bucket                 string `json:"bucket" yaml:"bucket"`
	Region                 string `json:"region" yaml:"region"`
	AccessKeyID            string `json:"access_key_id" yaml:"access_key_id"`
	SecretAccessKey        string `json:"secret_access_key" yaml:"secret_access_key"`
	DeleteLocalAfterUpload bool   `json:"delete_local_after_upload" yaml:"delete_local_after_upload"`
	Workers                int    `json:"workers" yaml:"workers"`

	// DePIN go-sdk storage settings
	DePINIdentityDir  string `json:"depin_identity_dir" yaml:"depin_identity_dir"`
	DePINCoordPeerURL string `json:"depin_coord_peer_url" yaml:"depin_coord_peer_url"`
	DePINPieceKeyPath string `json:"depin_piece_key_path" yaml:"depin_piece_key_path"`
	DePINLinkEndpoint string `json:"depin_link_endpoint" yaml:"depin_link_endpoint"`
}

// ProviderFactory is a function signature for constructing a StorageProvider instance.
type ProviderFactory func(ctx context.Context, cfg StorageConfig) (StorageProvider, error)

var (
	registryMutex sync.RWMutex
	registryMap   = make(map[ProviderType]ProviderFactory)
)

// RegisterProvider registers a ProviderFactory for a given ProviderType constant.
func RegisterProvider(providerType ProviderType, factory ProviderFactory) {
	registryMutex.Lock()
	defer registryMutex.Unlock()
	registryMap[ProviderType(strings.ToLower(string(providerType)))] = factory
}

// ProviderSelector selects and builds the appropriate StorageProvider using the registered provider map.
type ProviderSelector struct{}

// NewProviderSelector creates a new ProviderSelector instance.
func NewProviderSelector() *ProviderSelector {
	return &ProviderSelector{}
}

// Select constructs and returns the matching StorageProvider implementation from the registry map.
func (s *ProviderSelector) Select(ctx context.Context, cfg StorageConfig) (StorageProvider, error) {
	rawName := getResolvedValue(cfg.Provider, "STORAGE_PROVIDER", "MTX_STORAGE_PROVIDER", "LIVE_HLS_STORAGE_PROVIDER", "")
	providerType := ProviderType(strings.ToLower(strings.TrimSpace(rawName)))

	if providerType == "local" || providerType == "none" || providerType == "" {
		return nil, fmt.Errorf("local storage is default (external provider upload disabled)")
	}

	registryMutex.RLock()
	factory, exists := registryMap[providerType]
	registryMutex.RUnlock()

	if !exists {
		return nil, fmt.Errorf("unsupported storage provider '%s'. Registered providers: %v", providerType, s.RegisteredProviders())
	}

	return factory(ctx, cfg)
}

// RegisteredProviders returns a list of registered provider names.
func (s *ProviderSelector) RegisteredProviders() []string {
	registryMutex.RLock()
	defer registryMutex.RUnlock()

	names := make([]string, 0, len(registryMap))
	for name := range registryMap {
		names = append(names, string(name))
	}
	return names
}

func getResolvedValue(cfgVal string, keysAndDefault ...string) string {
	if strings.TrimSpace(cfgVal) != "" {
		return cfgVal
	}
	if len(keysAndDefault) == 0 {
		return ""
	}
	for i := 0; i < len(keysAndDefault)-1; i++ {
		if val := os.Getenv(keysAndDefault[i]); val != "" {
			return val
		}
	}
	return keysAndDefault[len(keysAndDefault)-1]
}
