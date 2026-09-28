package rtmp

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// publishWebhookClient requests publish webhooks. The timeout is per request,
// through the context.
var publishWebhookClient = &http.Client{}

// callPublishWebhook requests the publish webhook for a connection that is
// about to publish to pathName, and returns an error unless it answers 2xx.
//
// The webhook is how the API learns a stream exists: it reads the connection
// back through the control API, registers the media session and starts
// billing it. A stream it did not register must not go live, so every failure
// refuses the publish.
//
// The URL carries the webhook token, so it is never logged.
func (c *conn) callPublishWebhook(pathName string, streamKey string) error {
	vars := map[string]string{
		"MTX_CONN_TYPE":  string(c.APISourceDescribe().Type),
		"MTX_CONN_ID":    c.uuid.String(),
		"MTX_PATH":       pathName,
		"AIOZ_StreamKey": streamKey,
	}
	u := os.Expand(c.parent.PublishWebhook, func(name string) string {
		if v, ok := vars[name]; ok {
			return v
		}
		return os.Getenv(name)
	})

	timeout := time.Duration(c.parent.PublishWebhookTimeout)
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	ctx, cancel := context.WithTimeout(c.ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return fmt.Errorf("publish webhook: invalid URL")
	}

	res, err := publishWebhookClient.Do(req)
	if err != nil {
		// The error text of a failed request includes the URL.
		if ctx.Err() != nil {
			return fmt.Errorf("publish webhook: no answer within %v", timeout)
		}
		return fmt.Errorf("publish webhook: request failed")
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode > 299 {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 512))
		return fmt.Errorf(
			"publish webhook: status %d: %s",
			res.StatusCode,
			body,
		)
	}

	io.Copy(io.Discard, io.LimitReader(res.Body, 64*1024)) //nolint:errcheck
	return nil
}
