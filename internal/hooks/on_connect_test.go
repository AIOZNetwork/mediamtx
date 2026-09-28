package hooks

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsTranscoderChildPath(t *testing.T) {
	for _, ca := range []struct {
		path string
		want bool
	}{
		{path: "stream", want: false},
		{path: "nested/stream", want: false},
		{path: "stream/original", want: true},
		{path: "stream/1080", want: true},
		{path: "stream/720p", want: true},
		{path: "stream/video/original", want: true},
		{path: "stream/video/1080", want: true},
		{path: "stream/audio/main", want: true},
	} {
		t.Run(ca.path, func(t *testing.T) {
			require.Equal(t, ca.want, IsTranscoderChildPath(ca.path))
		})
	}
}
