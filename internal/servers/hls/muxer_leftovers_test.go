package hls

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRemoveMuxerLeftoversClearsEverythingAndDirRemovable(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{
		"a1b2_video1_init.mp4", "index.m3u8", "video1_stream.m3u8",
		"a1b2_video1_seg0.mp4", "x.m4s", "y.ts",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	removeMuxerLeftovers(dir)
	if left, _ := filepath.Glob(filepath.Join(dir, "*")); len(left) != 0 {
		t.Fatalf("leftovers: %v", left)
	}
	if err := os.Remove(dir); err != nil {
		t.Fatalf("dir should be removable: %v", err)
	}
}

func TestRemoveMuxerLeftoversNeverRecurses(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "720")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	inner := filepath.Join(sub, "a_init.mp4")
	other := filepath.Join(dir, "notes.txt")
	for _, p := range []string{inner, other} {
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	removeMuxerLeftovers(dir)
	for _, p := range []string{inner, other} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("%s must stay: %v", p, err)
		}
	}
}
