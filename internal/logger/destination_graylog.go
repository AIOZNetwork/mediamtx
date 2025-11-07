package logger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"time"
)

type LogLevel int

const (
	DEBUG LogLevel = 7
	INFO  LogLevel = 6
	WARN  LogLevel = 5
	ERROR LogLevel = 4
	FATAL LogLevel = 3
)

type GraylogLogger struct {
	ServiceName  string
	FacilityName string
	GraylogURL   string
	HTTPClient   *http.Client
}

// close implements destination.
func (gl *GraylogLogger) close() {
	// panic("unimplemented")
}

type LogMessage struct {
	Version      string         `json:"version"`
	Host         string         `json:"host"`
	ShortMessage string         `json:"short_message"`
	FullMessage  string         `json:"full_message,omitempty"`
	Timestamp    float64        `json:"timestamp"`
	Level        int            `json:"level"`
	Facility     string         `json:"facility"`
	Service      string         `json:"_service"`
	Extra        map[string]any `json:"-"`
}

func newDestinationGraylog(
	facilityName, serviceName, graylogURL string,
) destination {
	return &GraylogLogger{
		FacilityName: facilityName,
		ServiceName:  serviceName,
		GraylogURL:   graylogURL,
		HTTPClient:   &http.Client{},
	}
}

func (gl *GraylogLogger) log(
	t time.Time,
	level Level,
	format string,
	args ...interface{},
) {
	message := fmt.Sprintf(format, args...)
	fullMessage := message
	attrs := make(map[string]any)

	switch level {
	case Error:
		gl.Error(message, fullMessage, attrs)
	case Warn:
		gl.Warn(message, attrs)
	case Info:
		gl.Info(message, attrs)
	case Debug:
		gl.Debug(message, attrs)
	}
}

func removeAllANSICodes(str string) string {
	// This regex matches all ANSI escape sequences, not just color codes
	re := regexp.MustCompile(`\x1b\[[0-9;]*m`)
	return re.ReplaceAllString(str, "")
}

// sendLog sends a log message to Graylog
func (gl *GraylogLogger) sendLog(
	level LogLevel,
	message string,
	fullMessage string,
	extra map[string]any,
) error {
	loggedAt, ok := extra["logged_at"].(time.Time)
	if !ok {
		loggedAt = time.Now()
	} else {
		delete(extra, "logged_at")
	}

	logMsg := &LogMessage{
		Version:      "1.1",
		Host:         gl.ServiceName,
		ShortMessage: message,
		FullMessage:  fullMessage,
		Timestamp:    float64(loggedAt.Unix()),
		Level:        int(level),
		Facility:     gl.FacilityName,
		Service:      gl.ServiceName,
		Extra:        extra,
	}

	jsonData, err := json.Marshal(logMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal log message: %v", err)
	}

	resp, err := gl.HTTPClient.Post(
		gl.GraylogURL,
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		return fmt.Errorf("failed to send log to Graylog: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK &&
		resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("Graylog returned status: %d", resp.StatusCode)
	}

	return nil
}

// Debug logs a debug message
func (gl *GraylogLogger) Debug(message string, extra ...map[string]any) {
	extraFields := make(map[string]any)
	if len(extra) > 0 {
		extraFields = extra[0]
	}

	gl.sendLog(DEBUG, message, "", extraFields)
}

// Info logs an info message
func (gl *GraylogLogger) Info(message string, extra ...map[string]any) {
	extraFields := make(map[string]any)
	if len(extra) > 0 {
		extraFields = extra[0]
	}

	gl.sendLog(INFO, message, "", extraFields)
}

// Warn logs a warning message
func (gl *GraylogLogger) Warn(message string, extra ...map[string]any) {
	extraFields := make(map[string]any)
	if len(extra) > 0 {
		extraFields = extra[0]
	}

	gl.sendLog(WARN, message, "", extraFields)
}

// Error logs an error message
func (gl *GraylogLogger) Error(
	message string,
	fullMessage string,
	extra ...map[string]any,
) {
	extraFields := make(map[string]any)
	if len(extra) > 0 {
		extraFields = extra[0]
	}

	gl.sendLog(ERROR, message, fullMessage, extraFields)
}

// Fatal logs a fatal message
func (gl *GraylogLogger) Fatal(
	message string,
	fullMessage string,
	extra ...map[string]any,
) {
	extraFields := make(map[string]any)
	if len(extra) > 0 {
		extraFields = extra[0]
	}

	gl.sendLog(FATAL, message, fullMessage, extraFields)
}
