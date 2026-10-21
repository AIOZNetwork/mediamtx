package hls

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRemoveMuxerLeftoversClearsInitAndPlaylists(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{
		"a1b2_video1_init.mp4", "a1b2_audio2_init.mp4",
		"index.m3u8", "video1_stream.m3u8", "audio2_stream.m3u8",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	removeMuxerLeftovers(dir)

	left, _ := filepath.Glob(filepath.Join(dir, "*"))
	if len(left) != 0 {
		t.Fatalf("leftovers not removed: %v", left)
	}
	if err := os.Remove(dir); err != nil {
		t.Fatalf("directory should now be removable: %v", err)
	}
}

func TestRemoveMuxerLeftoversKeepsSegments(t *testing.T) {
	dir := t.TempDir()
	seg := filepath.Join(dir, "a1b2_video1_seg0.mp4")
	if err := os.WriteFile(seg, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	removeMuxerLeftovers(dir)
	if _, err := os.Stat(seg); err != nil {
		t.Fatalf("segment must stay: %v", err)
	}
}
