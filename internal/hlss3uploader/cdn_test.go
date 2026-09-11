package hlss3uploader

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCDNStorageProvider_Registration(t *testing.T) {
	selector := NewProviderSelector()
	registered := selector.RegisteredProviders()
	require.Contains(t, registered, string(ProviderCDN))
	require.Contains(t, registered, string(ProviderDePIN))
	require.Contains(t, registered, string(ProviderS3))
}

func TestCDNStorageProvider_Validation(t *testing.T) {
	selector := NewProviderSelector()

	// Missing endpoint
	_, err := selector.Select(context.Background(), StorageConfig{
		Provider:  "cdn",
		CDNHubURL: "https://hub.aioz.network",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "endpoint url is required")

	// Missing hub url
	_, err = selector.Select(context.Background(), StorageConfig{
		Provider:    "cdn",
		CDNEndpoint: "https://node.aioz.network",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "hub url is required")
}

func TestCDNStorageProvider_Accessors(t *testing.T) {
	p, err := NewCDNStorageProvider("https://node.aioz.network/", "https://hub.aioz.network/", "0x123456")
	require.NoError(t, err)

	require.Equal(t, "cdn", p.Name())
	require.Equal(t, "https://node.aioz.network", p.CDNURL())
	require.Equal(t, "https://hub.aioz.network", p.HubURL())
	require.Equal(t, "0x123456", p.BusinessAddress())
	require.NotNil(t, p.Helper())
	require.NoError(t, p.DeleteFolder(context.Background(), "live-hls/stream1"))
	require.NoError(t, p.Close())
}

func TestCDNStorageProvider_UploadFile(t *testing.T) {
	expectedFileID := "test-file-uuid-001"
	expectedOffset := uint64(0)
	expectedSize := uint64(1024)

	cdnServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/packUpload" && r.Method == http.MethodPost {
			mr, err := r.MultipartReader()
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			part, err := mr.NextPart()
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			require.Equal(t, "file", part.FormName())
			content, _ := io.ReadAll(part)
			require.NotEmpty(t, content)

			resp := UploadFileResponse{
				FileRecord: fileRecord{
					ID: expectedFileID,
				},
				ZipHeader: zipHeader{
					File: []struct {
						Name               string `json:"Name"`
						CompressedSize     uint32 `json:"CompressedSize"`
						UncompressedSize   uint32 `json:"UncompressedSize"`
						CompressedSize64   uint64 `json:"CompressedSize64"`
						UncompressedSize64 uint64 `json:"UncompressedSize64"`
						Offset             uint64 `json:"Offset"`
					}{
						{
							Name:               part.FileName(),
							Offset:             expectedOffset,
							UncompressedSize64: expectedSize,
						},
					},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
		http.NotFound(w, r)
	}))
	defer cdnServer.Close()

	p, err := NewCDNStorageProvider(cdnServer.URL, "https://hub.aioz.network", "")
	require.NoError(t, err)

	tmpDir := t.TempDir()
	localFile := filepath.Join(tmpDir, "stream1_00001.mp4")
	sampleData := []byte("fmp4 segment mock content bytes")
	require.NoError(t, os.WriteFile(localFile, sampleData, 0o644))

	remoteKey := "live-hls/stream1/stream1_00001.mp4"
	etag, err := p.UploadFile(context.Background(), localFile, remoteKey, "video/mp4")
	require.NoError(t, err)

	expectedETag := fmt.Sprintf("%s:%d:%d", expectedFileID, expectedOffset, expectedSize)
	require.Equal(t, expectedETag, etag)

	obj, ok := p.getObject(remoteKey)
	require.True(t, ok)
	require.Equal(t, expectedFileID, obj.Id)
	require.Equal(t, int64(expectedOffset), obj.Offset)
	require.Equal(t, int64(expectedSize), obj.Size)
}

func TestCDNStorageProvider_GetLink_And_Cache(t *testing.T) {
	fileID := "file-uuid-getlink-test"
	offset := int64(64)
	size := int64(2048)
	sampleSigStdBase64 := base64.StdEncoding.EncodeToString([]byte("super-secure-signature-bytes+/="))
	expectedURLSig := base64.RawURLEncoding.EncodeToString([]byte("super-secure-signature-bytes+/="))
	expireAt := time.Now().Add(2 * time.Hour).UnixNano()

	var getTicketCalls int32

	cdnServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/getFileRecord/") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(GetFileRecordResponse{
				Id:     fileID,
				Status: FileRecordReadyStatus,
			})
			return
		}
		if r.URL.Path == "/getTicket" && r.Method == http.MethodGet {
			atomic.AddInt32(&getTicketCalls, 1)
			q := r.URL.Query()
			require.Equal(t, fileID, q.Get("id"))
			require.Equal(t, fmt.Sprintf("%d,%d", offset, size), q.Get("range"))

			resp := GetTicketResponse{
				Version:      1,
				FileRecordId: fileID,
				Signature:    sampleSigStdBase64,
				ExpiredAt:    expireAt,
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
		http.NotFound(w, r)
	}))
	defer cdnServer.Close()

	hubURL := "https://hub.aioz.network"
	p, err := NewCDNStorageProvider(cdnServer.URL, hubURL, "")
	require.NoError(t, err)

	remoteKey := "live-hls/stream1/seg0.mp4"
	p.RegisterKeyFileInfo(remoteKey, fileID, offset, size)

	// First call - should hit mock server
	link1, err := p.GetLink(context.Background(), remoteKey)
	require.NoError(t, err)
	expectedLink := fmt.Sprintf("%s/file/%s?expire=%d&signature=%s&range=%d,%d",
		hubURL, fileID, expireAt, expectedURLSig, offset, size)
	require.Equal(t, expectedLink, link1)
	require.Equal(t, int32(1), atomic.LoadInt32(&getTicketCalls))

	// Second call - should hit cache
	link2, err := p.GetLink(context.Background(), remoteKey)
	require.NoError(t, err)
	require.Equal(t, expectedLink, link2)
	require.Equal(t, int32(1), atomic.LoadInt32(&getTicketCalls))
}

func TestCDNStorageProvider_GetLink_ETagFallback(t *testing.T) {
	fileID := "direct-etag-uuid"
	offset := int64(128)
	size := int64(4096)
	rawSig := []byte("etag-fallback-sig")
	sampleSigStd := base64.StdEncoding.EncodeToString(rawSig)
	expectedURLSig := base64.RawURLEncoding.EncodeToString(rawSig)
	expireAt := time.Now().Add(1 * time.Hour).UnixNano()

	cdnServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/getFileRecord/") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(GetFileRecordResponse{
				Id:     fileID,
				Status: FileRecordReadyStatus,
			})
			return
		}
		if r.URL.Path == "/getTicket" {
			resp := GetTicketResponse{
				Version:      1,
				FileRecordId: fileID,
				Signature:    sampleSigStd,
				ExpiredAt:    expireAt,
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
		http.NotFound(w, r)
	}))
	defer cdnServer.Close()

	hubURL := "https://hub.aioz.network"
	p, err := NewCDNStorageProvider(cdnServer.URL, hubURL, "")
	require.NoError(t, err)

	// Call GetLink using ETag string directly without prior registration
	etagKey := fmt.Sprintf("%s:%d:%d", fileID, offset, size)
	link, err := p.GetLink(context.Background(), etagKey)
	require.NoError(t, err)
	expectedLink := fmt.Sprintf("%s/file/%s?expire=%d&signature=%s&range=%d,%d",
		hubURL, fileID, expireAt, expectedURLSig, offset, size)
	require.Equal(t, expectedLink, link)
}

func TestCDNStorageProvider_GetObject(t *testing.T) {
	fileID := "proxy-object-file-id"
	offset := int64(0)
	size := int64(12)
	expectedData := []byte("hello stream")

	cdnServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/cacheFile/"+fileID {
			require.Equal(t, fmt.Sprintf("%d,%d", offset, size), r.URL.Query().Get("range"))
			w.Header().Set("Content-Type", "video/mp4")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(expectedData)
			return
		}
		http.NotFound(w, r)
	}))
	defer cdnServer.Close()

	p, err := NewCDNStorageProvider(cdnServer.URL, "https://hub.aioz.network", "")
	require.NoError(t, err)

	storageKey := "live-hls/test/seg.mp4"
	p.RegisterKeyFileInfo(storageKey, fileID, offset, size)

	reader, contentType, length, err := p.GetObject(context.Background(), storageKey)
	require.NoError(t, err)
	defer reader.Close()

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, expectedData, body)
	require.Equal(t, "video/mp4", contentType)
	require.Equal(t, size, length)
}

func TestCDNStorageProvider_RegisterKeyETag(t *testing.T) {
	p, err := NewCDNStorageProvider("https://node.aioz.network", "https://hub.aioz.network", "")
	require.NoError(t, err)

	p.RegisterKeyETag("live-hls/stream1/init.mp4", "file-uuid-init:0:800")
	obj, ok := p.getObject("live-hls/stream1/init.mp4")
	require.True(t, ok)
	require.Equal(t, "file-uuid-init", obj.Id)
	require.Equal(t, int64(0), obj.Offset)
	require.Equal(t, int64(800), obj.Size)

	// Invalid ETag should not crash and not store invalid info
	p.RegisterKeyETag("live-hls/stream1/invalid.mp4", "invalid-format")
	_, ok = p.keyToObj.Load("live-hls/stream1/invalid.mp4")
	require.False(t, ok)
}

func TestCdnHelper_DirectMethodsMatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/currentPrice":
			_ = json.NewEncoder(w).Encode(GetAiozPriceResponse{AiozPrice: "0.55"})
		case r.URL.Path == "/getBalance":
			_ = json.NewEncoder(w).Encode(GetBalanceResponse{
				DepositAddress: "0x123",
				SetCreditLater: true,
			})
		case strings.HasPrefix(r.URL.Path, "/endFileRecord"):
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	helper := MustNewCdnHelper(server.URL, "https://hub.aioz.network", "0x123")
	price, err := helper.GetAIOZPrice(context.Background())
	require.NoError(t, err)
	require.Equal(t, 0.55, price)

	err = helper.getBalance()
	require.NoError(t, err)

	err = helper.Delete(context.Background(), &Object{Id: "test-delete-id"})
	require.NoError(t, err)
}
