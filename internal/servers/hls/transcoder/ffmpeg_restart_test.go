package transcoder

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/test"
)

// fakeFFmpeg puts an ffmpeg on PATH that records each start in a file and
// then runs script.
func fakeFFmpeg(t *testing.T, script string) (startsFile string) {
	dir := t.TempDir()
	startsFile = filepath.Join(dir, "starts")

	err := os.WriteFile(filepath.Join(dir, "ffmpeg"),
		[]byte("#!/bin/sh\necho start >> "+startsFile+"\n"+script+"\n"), 0o755)
	require.NoError(t, err)

	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return startsFile
}

func countStarts(t *testing.T, startsFile string) int {
	buf, err := os.ReadFile(startsFile)
	if os.IsNotExist(err) {
		return 0
	}
	require.NoError(t, err)
	return strings.Count(string(buf), "start")
}

func shortBackoff(t *testing.T) {
	prevMin, prevMax := restartMinBackoff, restartMaxBackoff
	restartMinBackoff, restartMaxBackoff = 50*time.Millisecond, 100*time.Millisecond
	t.Cleanup(
		func() { restartMinBackoff, restartMaxBackoff = prevMin, prevMax },
	)
}

func newTestTranscoder() *FFmpegTranscoder {
	return NewFFmpegTranscoder(&conf.Path{
		HLSTranscoding: true,
		HLSTranscodingRenditions: []conf.HLSTranscodingRendition{
			{Name: "480", Width: 854, Height: 480},
		},
	}, "stream", test.NilLogger, ":1935", ":8554")
}

// TestFFmpegTranscoderRestartsWhileLive: FFmpeg that exits by itself while the
// stream is live - dropped by the server for falling behind - is started
// again, and Stop ends that for good.
func TestFFmpegTranscoderRestartsWhileLive(t *testing.T) {
	shortBackoff(t)
	startsFile := fakeFFmpeg(t, "sleep 0.1; exit 1")

	tr := newTestTranscoder()
	require.NoError(t, tr.Start())

	require.Eventually(t, func() bool {
		return countStarts(t, startsFile) >= 3
	}, 5*time.Second, 20*time.Millisecond)

	tr.Stop()
	stopped := countStarts(t, startsFile)

	time.Sleep(500 * time.Millisecond)
	require.Equal(
		t,
		stopped,
		countStarts(t, startsFile),
		"no restart after Stop",
	)
}

// TestFFmpegTranscoderStopWhileRunning: Stop ends a running FFmpeg promptly
// and does not start another.
func TestFFmpegTranscoderStopWhileRunning(t *testing.T) {
	shortBackoff(t)
	startsFile := fakeFFmpeg(t, "exec sleep 30")

	tr := newTestTranscoder()
	require.NoError(t, tr.Start())

	require.Eventually(t, func() bool {
		return countStarts(t, startsFile) == 1
	}, 5*time.Second, 20*time.Millisecond)

	begin := time.Now()
	tr.Stop()
	require.Less(t, time.Since(begin), 3*time.Second)

	time.Sleep(300 * time.Millisecond)
	require.Equal(t, 1, countStarts(t, startsFile))
}
