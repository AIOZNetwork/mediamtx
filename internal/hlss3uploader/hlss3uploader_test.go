package hlss3uploader

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockProvider struct {
	name        string
	uploadCount int32
	uploadErr   error
	delay       time.Duration
}

func (m *mockProvider) Name() string {
	if m.name == "" {
		return "mock"
	}
	return m.name
}

func (m *mockProvider) UploadFile(ctx context.Context, localPath, remoteKey, contentType string) (string, error) {
	atomic.AddInt32(&m.uploadCount, 1)
	if m.delay > 0 {
		select {
		case <-time.After(m.delay):
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if m.uploadErr != nil {
		return "", m.uploadErr
	}
	return "mock-etag", nil
}

func (m *mockProvider) DeleteFolder(ctx context.Context, remoteKey string) error {
	return nil
}

func (m *mockProvider) Close() error {
	return nil
}

func TestHLSS3Uploader_InFlightAndCompletedExclusion(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "uploader-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	filePath := filepath.Join(tmpDir, "segment_001.m4s")
	if err := os.WriteFile(filePath, []byte("fake video content"), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	provider := &mockProvider{
		delay: 100 * time.Millisecond,
	}

	uploader := &HLSS3Uploader{
		Config: StorageConfig{
			Directory: tmpDir,
			Prefix:    "test-prefix",
		},
		provider: provider,
		ctx:      context.Background(),
	}

	expectedKey := "test-prefix/segment_001.m4s"

	// 1. Concurrent processFile calls for the same file
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			uploader.processFile(filePath)
		}()
	}
	wg.Wait()

	// Only 1 upload should have occurred due to in-flight exclusion
	if count := atomic.LoadInt32(&provider.uploadCount); count != 1 {
		t.Errorf("expected 1 upload call, got %d", count)
	}

	// Verify expectedKey is now in uploadedFiles
	if _, loaded := uploader.uploadedFiles.Load(expectedKey); !loaded {
		t.Errorf("expected key %q to be stored in uploadedFiles", expectedKey)
	}

	// Verify expectedKey is cleared from processingFiles
	if _, loaded := uploader.processingFiles.Load(expectedKey); loaded {
		t.Errorf("expected key %q to be cleared from processingFiles", expectedKey)
	}

	// 2. Subsequent processFile call should be skipped because of uploadedFiles
	uploader.processFile(filePath)
	if count := atomic.LoadInt32(&provider.uploadCount); count != 1 {
		t.Errorf("expected upload count to remain 1 after completed exclusion, got %d", count)
	}
}

func TestHLSS3Uploader_FailedUploadReleasesLockForRetry(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "uploader-fail-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	filePath := filepath.Join(tmpDir, "segment_002.m4s")
	if err := os.WriteFile(filePath, []byte("fake video content"), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	provider := &mockProvider{
		uploadErr: errors.New("network failure"),
	}

	uploader := &HLSS3Uploader{
		Config: StorageConfig{
			Directory: tmpDir,
			Prefix:    "test-prefix",
		},
		provider: provider,
		ctx:      context.Background(),
	}

	expectedKey := "test-prefix/segment_002.m4s"

	// 1. First attempt fails
	uploader.processFile(filePath)

	if count := atomic.LoadInt32(&provider.uploadCount); count != 1 {
		t.Fatalf("expected 1 upload attempt, got %d", count)
	}

	// Should NOT be in uploadedFiles
	if _, loaded := uploader.uploadedFiles.Load(expectedKey); loaded {
		t.Errorf("expected key %q NOT to be in uploadedFiles after failure", expectedKey)
	}

	// Should NOT be in processingFiles (lock released)
	if _, loaded := uploader.processingFiles.Load(expectedKey); loaded {
		t.Errorf("expected key %q to be cleared from processingFiles after failure", expectedKey)
	}

	// 2. Retry attempt succeeds after clearing error
	provider.uploadErr = nil
	uploader.processFile(filePath)

	if count := atomic.LoadInt32(&provider.uploadCount); count != 2 {
		t.Fatalf("expected 2 upload attempts after retry, got %d", count)
	}

	// Now should be in uploadedFiles
	if _, loaded := uploader.uploadedFiles.Load(expectedKey); !loaded {
		t.Errorf("expected key %q to be in uploadedFiles after retry success", expectedKey)
	}
}
