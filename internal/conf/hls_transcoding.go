package conf

// HLSTranscodingRendition is one output of the HLS transcoder.
type HLSTranscodingRendition struct {
	Name         string `json:"name"`
	Width        int    `json:"width"`
	Height       int    `json:"height"`
	VideoBitrate string `json:"videoBitrate"`
}
