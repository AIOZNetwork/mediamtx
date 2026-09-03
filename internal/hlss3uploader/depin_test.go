package hlss3uploader

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDePINStorageProvider_Registration(t *testing.T) {
	selector := NewProviderSelector()
	registered := selector.RegisteredProviders()
	require.Contains(t, registered, string(ProviderDePIN))
	require.Contains(t, registered, string(ProviderS3))
}

func TestDePINStorageProvider_FactoryValidation(t *testing.T) {
	selector := NewProviderSelector()

	// Missing required fields should fail
	_, err := selector.Select(context.Background(), StorageConfig{
		Provider: "depin",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "identity_dir and coord_peer_url are required")

	// Missing coordPeerURL should fail
	_, err = selector.Select(context.Background(), StorageConfig{
		Provider:         "depin",
		DePINIdentityDir: "/some/dir",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "identity_dir and coord_peer_url are required")
}

func TestDePINStorageProvider_Accessors(t *testing.T) {
	provider := &DePINStorageProvider{
		contractID:   "test-contract-123",
		linkEndpoint: "http://localhost:8080",
	}

	require.Equal(t, "depin", provider.Name())
	require.Equal(t, "test-contract-123", provider.ContractID())
	require.Equal(t, "http://localhost:8080", provider.LinkEndpoint())
}

func TestDePINStorageProvider_UploadFile_RejectsEmptyContract(t *testing.T) {
	provider := &DePINStorageProvider{
		contractID: "",
	}

	_, err := provider.UploadFile(context.Background(), "/some/file.mp4", "live-hls/stream1/seg1.mp4", "video/mp4")
	require.Error(t, err)
	require.Contains(t, err.Error(), "contract ID is mandatory but is empty")
}

func TestDePINStorageProvider_UploadFile_RejectsInvalidContract(t *testing.T) {
	provider := &DePINStorageProvider{
		contractID: "invalid-uuid-string",
	}

	_, err := provider.UploadFile(context.Background(), "/some/file.mp4", "live-hls/stream1/seg1.mp4", "video/mp4")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid contract ID")
}

func TestDePINStorageProvider_GetObject_UnknownKey(t *testing.T) {
	provider := &DePINStorageProvider{
		contractID: "00000000-0000-0000-0000-000000000001",
	}

	_, _, _, err := provider.GetObject(context.Background(), "non-existent-segment.mp4")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown key")
}

func TestDePINStorageProvider_DeleteFolder_EmptyContract(t *testing.T) {
	provider := &DePINStorageProvider{
		contractID: "",
	}

	err := provider.DeleteFolder(context.Background(), "live-hls/stream1")
	require.NoError(t, err)
}

func TestDePINStorageProvider_Close_NilClient(t *testing.T) {
	provider := &DePINStorageProvider{
		client: nil,
	}

	err := provider.Close()
	require.NoError(t, err)
}

func TestDePINStorageProvider_UploadAndGetMockFlow(t *testing.T) {
	tmpDir := t.TempDir()
	sampleFile := filepath.Join(tmpDir, "segment_001.mp4")
	sampleContent := []byte("fmp4 segment test content")
	err := os.WriteFile(sampleFile, sampleContent, 0o644)
	require.NoError(t, err)

	provider := &DePINStorageProvider{
		contractID: "00000000-0000-0000-0000-000000000001",
	}
	// Simulate stored UUID mapping
	provider.RegisterKeyUUID("live-hls/stream1/segment_001.mp4", "00000000-0000-0000-0000-000000000002")

	uuidVal, ok := provider.keyToUUID.Load("live-hls/stream1/segment_001.mp4")
	require.True(t, ok)
	require.Equal(t, "00000000-0000-0000-0000-000000000002", uuidVal)
}

func TestDePINStorageProvider_GetLink_CachedLink(t *testing.T) {
	provider := &DePINStorageProvider{
		linkEndpoint: "https://edge.aioz.network",
		contractID:   "00000000-0000-0000-0000-000000000001",
	}

	targetUUID := "00000000-0000-0000-0000-000000000002"
	provider.RegisterKeyUUID("live-hls/stream1/seg001.mp4", targetUUID)

	expectedLink := "https://edge.aioz.network/download/" + targetUUID + "?ticket=mockTicket"
	provider.linkCache.Store(targetUUID, cachedGoSdkLink{
		link:      expectedLink,
		expiresAt: time.Now().Add(2 * time.Hour).UnixNano(),
	})

	link, err := provider.GetLink(context.Background(), "live-hls/stream1/seg001.mp4")
	require.NoError(t, err)
	require.Equal(t, expectedLink, link)
}

func TestDePINStorageProvider_GetLink_InvalidUUID(t *testing.T) {
	provider := &DePINStorageProvider{
		contractID: "00000000-0000-0000-0000-000000000001",
	}

	_, err := provider.GetLink(context.Background(), "invalid-uuid-key")
	require.Error(t, err)
	require.Contains(t, err.Error(), "parse uuid")
}
