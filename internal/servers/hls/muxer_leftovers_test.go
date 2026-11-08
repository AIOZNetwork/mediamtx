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

// An ABR stream has a base muxer in <root>/<id> and rendition muxers in
// <root>/<id>/<rendition>. The base muxer has no uploader and closes first,
// while a rendition muxer is still flushing uploads, so its directory is not
// empty yet. The last muxer to close must remove the then-empty <root>/<id>.
func TestRemoveMuxerDirectoryRemovesEmptyParentAfterLastChild(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, "4c1a7fda")
	child := filepath.Join(base, "240")
	if err := os.MkdirAll(child, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{
		filepath.Join(base, "a_video1_init.mp4"),
		filepath.Join(child, "b_video1_init.mp4"),
		filepath.Join(child, "b_video1_seg0.mp4"),
	} {
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	removeMuxerDirectory(root, base)
	if _, err := os.Stat(child); err != nil {
		t.Fatalf("rendition dir must survive the base muxer close: %v", err)
	}

	removeMuxerDirectory(root, child)
	if _, err := os.Stat(base); !os.IsNotExist(err) {
		t.Fatalf("empty stream dir must be removed after the last muxer closes, stat err: %v", err)
	}
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("hlsDirectory root must never be removed: %v", err)
	}
}

func TestRemoveMuxerDirectoryKeepsNonEmptyParent(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, "4c1a7fda")
	for _, name := range []string{"240", "480"} {
		if err := os.MkdirAll(filepath.Join(base, name), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	removeMuxerDirectory(root, filepath.Join(base, "240"))
	if _, err := os.Stat(filepath.Join(base, "480")); err != nil {
		t.Fatalf("sibling rendition dir must stay: %v", err)
	}
}
