package hlss3uploader

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

var cdnLogger *slog.Logger = slog.Default()

var UploadSpeed int64 = 10 * 100

var FileRecordReadyStatus int = 2

type Object struct {
	Id     string
	Offset int64
	Size   int64
	Name   string
}

type fileRecord struct {
	ID       string `json:"ID"`
	Owner    string `json:"owner"`
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	ReaderID int    `json:"readerId"`
}

type zipHeader struct {
	File []struct {
		Name               string `json:"Name"`
		CompressedSize     uint32 `json:"CompressedSize"`
		UncompressedSize   uint32 `json:"UncompressedSize"`
		CompressedSize64   uint64 `json:"CompressedSize64"`
		UncompressedSize64 uint64 `json:"UncompressedSize64"`
		Offset             uint64 `json:"Offset"`
	} `json:"File"`
}

type UploadFileResponse struct {
	FileRecord fileRecord
	ZipHeader  zipHeader
}

type GetTicketResponse struct {
	Version          int    `json:"version"`
	FileRecordId     string `json:"file_record_id"`
	Signature        string `json:"signature"`
	DecodedSignature string `json:"-"`
	ExpiredAt        int64  `json:"expire_at_ns"`
}

type GetFileRecordResponse struct {
	Id     string `json:"ID"`
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	Status int    `json:"status"`
}

type GetBalanceResponse struct {
	DepositAddress string `json:"deposit_address"`
	SetCreditLater bool   `json:"set_credit_later"`
}

type GetDetailBalanceResponse struct {
	Credit                 string `json:"credit"`
	DeliveryCreditExpense  string `json:"delivery_credit_expense"`
	DepositAddress         string `json:"deposit_address"`
	SetCreditLater         bool   `json:"set_credit_later"`
	StorageCreditExpense   string `json:"storage_credit_expense"`
	TranscodeCreditExpense string `json:"transcode_credit_expense"`
}

type UploadRawResponse struct {
	FileId string `json:"fileId"`
	Url    string `json:"url"`
}

type GetAiozPriceResponse struct {
	AiozPrice string `json:"aioz_price"`
}

var (
	TranscodingStatus = "transcoding"
	TranscodedStatus  = "transcoded"
	FailStatus        = "fail"
)

// CdnHelper provides exact 1:1 API client implementation of internal/utils/storage/cdn.go.
type CdnHelper struct {
	cdnUrl          string
	hubUrl          string
	businessAddress string
	ticketMapping   *sync.Map
	retry           int
}

type Option func(*CdnHelper)

func MustNewCdnHelper(
	cdnUrl, hubUrl, bussinessAddress string, options ...Option,
) *CdnHelper {
	helper := &CdnHelper{
		cdnUrl:          strings.TrimRight(strings.TrimSpace(cdnUrl), "/"),
		hubUrl:          strings.TrimRight(strings.TrimSpace(hubUrl), "/"),
		businessAddress: strings.TrimSpace(bussinessAddress),
		ticketMapping:   &sync.Map{},
		retry:           5,
	}

	for _, option := range options {
		option(helper)
	}

	if helper.retry < 1 {
		panic("retry must bigger than 1")
	}

	return helper
}

func WithRetry(retry int) Option {
	return func(h *CdnHelper) {
		h.retry = retry
	}
}

func WithTicketMapping(ticketMapping *sync.Map) Option {
	return func(h *CdnHelper) {
		h.ticketMapping = ticketMapping
	}
}

func (h *CdnHelper) handleRequest(
	ctx context.Context,
	req *http.Request,
	expectCode int,
	timeout time.Duration,
	needRetry bool,
	readers []io.Reader,
) (*http.Response, error) {
	transport := &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   3 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		ForceAttemptHTTP2:   true,
		TLSHandshakeTimeout: 3 * time.Second,
		MaxIdleConns:        100,
		IdleConnTimeout:     90 * time.Second,
	}

	client := http.Client{
		Transport: transport,
		Timeout:   max(10*time.Second, timeout),
	}

	var (
		count int
		resp  *http.Response
		err   error
	)

	start := time.Now().UTC()
	defer func() {
		diff := time.Since(start).Seconds()
		if diff >= timeout.Seconds()*0.5 {
			cdnLogger.WarnContext(
				ctx,
				"cdn response too long",
				slog.Any("url", req.URL.String()),
				slog.Any("response time", diff),
				slog.Any("retry time", count),
			)
		}
	}()

	for {
		resp, err = func() (*http.Response, error) {
			now := time.Now().UTC()

			resp, err = client.Do(req)
			if err != nil {
				return nil, err
			}

			if resp.StatusCode != expectCode {
				defer resp.Body.Close()
				respBody, err := io.ReadAll(resp.Body)
				if err != nil {
					cdnLogger.ErrorContext(
						ctx,
						"read response body error",
						slog.Any("err", err),
						slog.Any("url", req.URL.String()),
						slog.Any("responseTime", time.Since(now).Seconds()),
					)
				}

				return nil, fmt.Errorf(
					"Expect code is %d but cdn response: %d, body: %s, url:	%s",
					expectCode,
					resp.StatusCode,
					string(respBody),
					req.URL.String(),
				)
			}

			return resp, nil
		}()

		if err != nil {
			cdnLogger.ErrorContext(
				ctx,
				"cdn request error",
				slog.Any("err", err),
			)
			if count == h.retry || !needRetry {
				return nil, err
			}

			if len(readers) > 0 {
				body := new(bytes.Buffer)
				writer := multipart.NewWriter(body)
				for _, reader := range readers {
					if readSeeker, ok := reader.(io.ReadSeeker); ok {
						if _, err := readSeeker.Seek(0, io.SeekStart); err != nil {
							return nil, err
						}

						part, err := writer.CreateFormFile(
							"file",
							"file",
						)
						if err != nil {
							return nil, err
						}

						if n, err := io.Copy(part, readSeeker); err != nil || n == 0 {
							if n == 0 {
								return nil, errors.New("empty readers")
							}

							return nil, err
						}

					} else {
						return nil, err
					}
				}

				if err := writer.Close(); err != nil {
					return nil, err
				}
				newReq, err := http.NewRequest(
					req.Method,
					req.URL.String(),
					body,
				)
				if err != nil {
					return nil, err
				}

				newReq.Header.Add(
					"Content-Type",
					writer.FormDataContentType(),
				)

				req = newReq
			}

			count++
			continue
		}

		return resp, nil
	}
}

func (h *CdnHelper) Uploads(
	ctx context.Context,
	data string, files map[string]io.Reader,
) ([]*Object, int64, error) {
	now := time.Now().UTC()
	var timeout int64
	defer func() {
		cdnLogger.DebugContext(
			ctx,
			"Cdn upload files info",
			slog.Any("run time", time.Since(now).Seconds()),
			slog.Any("file count", len(files)),
			slog.Any("timeout", timeout),
		)
	}()

	body := new(bytes.Buffer)
	writer := multipart.NewWriter(body)
	var totalByte int64
	readers := make([]io.Reader, 0, len(files))
	for name, reader := range files {
		part, err := writer.CreateFormFile(
			"file",
			name,
		)
		if err != nil {
			return nil, 0, err
		}

		n, err := io.Copy(part, reader)
		if err != nil {
			return nil, 0, err
		}

		readers = append(readers, reader)
		totalByte += n
	}

	if err := writer.Close(); err != nil {
		return nil, 0, err
	}

	timeout = totalByte / UploadSpeed
	req, err := http.NewRequest(
		http.MethodPost,
		fmt.Sprintf("%s/packUpload", h.cdnUrl),
		body,
	)
	if err != nil {
		return nil, 0, err
	}

	req.Header.Add(
		"Content-Type",
		writer.FormDataContentType(),
	)
	resp, err := h.handleRequest(
		ctx,
		req,
		http.StatusOK,
		time.Duration(timeout)*time.Second,
		true,
		readers,
	)
	if err != nil {
		return nil, 0, err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf(
			"uploader response with code: %d",
			resp.StatusCode,
		)
	}

	var rs UploadFileResponse
	if err := json.NewDecoder(resp.Body).Decode(&rs); err != nil {
		return nil, 0, err
	}

	if len(rs.ZipHeader.File) == 0 {
		return nil, 0, fmt.Errorf("zip header's file is empty")
	}

	objects := make([]*Object, 0, len(rs.ZipHeader.File))
	for _, file := range rs.ZipHeader.File {
		objects = append(
			objects, &Object{
				Name:   file.Name,
				Id:     rs.FileRecord.ID,
				Offset: int64(file.Offset),
				Size:   int64(file.UncompressedSize64),
			},
		)
	}

	return objects, rs.FileRecord.Size, nil
}

func (h *CdnHelper) Upload(
	ctx context.Context,
	data string, reader io.Reader,
) (*Object, error) {
	now := time.Now().UTC()
	var timeout int64
	defer func() {
		cdnLogger.DebugContext(
			ctx,
			"Cdn upload file info",
			slog.Any("run time", time.Since(now).Seconds()),
			slog.Any("timeout", timeout),
		)
	}()

	body := new(bytes.Buffer)
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile(
		"file",
		data,
	)
	if err != nil {
		return nil, err
	}

	n, err := io.Copy(part, reader)
	if err != nil {
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	timeout = n / UploadSpeed
	req, err := http.NewRequest(
		http.MethodPost,
		fmt.Sprintf("%s/packUpload", h.cdnUrl),
		body,
	)
	if err != nil {
		return nil, err
	}

	req.Header.Add(
		"Content-Type",
		writer.FormDataContentType(),
	)
	resp, err := h.handleRequest(
		ctx,
		req,
		http.StatusOK,
		time.Duration(timeout)*time.Second,
		true,
		[]io.Reader{reader},
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"uploader response with code: %d",
			resp.StatusCode,
		)
	}

	var rs UploadFileResponse
	if err := json.NewDecoder(resp.Body).Decode(&rs); err != nil {
		return nil, err
	}

	if len(rs.ZipHeader.File) == 0 {
		return nil, fmt.Errorf("zip header's file is empty")
	}

	return &Object{
		Id:     rs.FileRecord.ID,
		Offset: int64(rs.ZipHeader.File[0].Offset),
		Size:   int64(rs.ZipHeader.File[0].UncompressedSize64),
	}, nil
}

func (h *CdnHelper) PackUploadByte(ctx context.Context, name string, data []byte) (*Object, error) {
	now := time.Now().UTC()
	var timeout int64
	defer func() {
		cdnLogger.DebugContext(
			ctx,
			"Cdn upload file info",
			slog.Any("run time", time.Since(now).Seconds()),
			slog.Any("timeout", timeout),
		)
	}()

	body := new(bytes.Buffer)
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", name)
	if err != nil {
		return nil, err
	}

	n, err := part.Write(data)
	if err != nil {
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	timeout = int64(n) / 10 / 1024
	req, err := http.NewRequest(
		http.MethodPost,
		fmt.Sprintf("%s/packUpload", h.cdnUrl),
		body,
	)
	if err != nil {
		return nil, err
	}

	req.Header.Add(
		"Content-Type",
		writer.FormDataContentType(),
	)
	resp, err := h.handleRequest(
		ctx,
		req,
		http.StatusOK,
		time.Duration(timeout)*time.Second,
		true,
		[]io.Reader{bytes.NewReader(data)},
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"uploader response with code: %d",
			resp.StatusCode,
		)
	}

	var rs UploadFileResponse
	if err := json.NewDecoder(resp.Body).Decode(&rs); err != nil {
		return nil, err
	}

	if len(rs.ZipHeader.File) == 0 {
		return nil, fmt.Errorf("zip header's file is empty")
	}

	return &Object{
		Id:     rs.FileRecord.ID,
		Offset: int64(rs.ZipHeader.File[0].Offset),
		Size:   int64(rs.ZipHeader.File[0].UncompressedSize64),
		Name:   name,
	}, nil
}

func (h *CdnHelper) UploadZip(
	ctx context.Context,
	data string, reader io.Reader,
) (*Object, error) {
	now := time.Now().UTC()
	var timeout int64
	defer func() {
		cdnLogger.DebugContext(
			ctx,
			"Cdn upload zip file info",
			slog.Any("run time", time.Since(now).Seconds()),
			slog.Any("timeout", timeout),
		)
	}()

	body := new(bytes.Buffer)
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile(
		"file",
		data,
	)
	if err != nil {
		return nil, err
	}

	n, err := io.Copy(part, reader)
	if err != nil {
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	timeout = n / UploadSpeed
	req, err := http.NewRequest(
		http.MethodPost,
		fmt.Sprintf("%s/packUpload?name=%s", h.cdnUrl, data),
		body,
	)
	if err != nil {
		return nil, err
	}

	req.Header.Add(
		"Content-Type",
		writer.FormDataContentType(),
	)
	resp, err := h.handleRequest(
		ctx,
		req,
		http.StatusOK,
		time.Duration(timeout)*time.Second,
		true,
		[]io.Reader{reader},
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"uploader response with code: %d",
			resp.StatusCode,
		)
	}

	var rs UploadFileResponse
	if err := json.NewDecoder(resp.Body).Decode(&rs); err != nil {
		return nil, err
	}

	if len(rs.ZipHeader.File) == 0 {
		return nil, fmt.Errorf("zip header's file is empty")
	}

	return &Object{
		Id:     rs.FileRecord.ID,
		Offset: int64(rs.ZipHeader.File[0].Offset),
		Size:   int64(rs.ZipHeader.File[0].UncompressedSize64),
	}, nil
}

func (h *CdnHelper) UploadRaw(
	ctx context.Context,
	data string, size int64, reader io.Reader,
) (*Object, error) {
	now := time.Now().UTC()
	var timeout int64
	defer func() {
		cdnLogger.DebugContext(
			ctx,
			"Cdn upload raw file info",
			slog.Any("run time", time.Since(now).Seconds()),
			slog.Any("timeout", timeout),
			slog.Any("size", size),
		)
	}()

	timeout = size / UploadSpeed
	req, err := http.NewRequest(
		http.MethodPost,
		fmt.Sprintf("%s/uploadRaw?name=%s", h.cdnUrl, data),
		reader,
	)
	if err != nil {
		return nil, err
	}

	resp, err := h.handleRequest(
		ctx,
		req,
		http.StatusOK,
		time.Duration(timeout)*time.Second,
		false,
		[]io.Reader{reader},
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"uploader response with code: %d",
			resp.StatusCode,
		)
	}

	var rs UploadRawResponse
	if err := json.NewDecoder(resp.Body).Decode(&rs); err != nil {
		return nil, err
	}

	return &Object{
		Id:     rs.FileId,
		Offset: 0,
		Size:   size,
	}, nil
}

func (h *CdnHelper) Download(ctx context.Context, obj *Object) (
	io.Reader, error,
) {
	now := time.Now().UTC()
	timeout := obj.Size / 10 / 1024
	defer func() {
		cdnLogger.DebugContext(
			ctx,
			"Cdn download file info",
			slog.Any("run time", time.Since(now).Seconds()),
			slog.Any("file id", obj.Id),
			slog.Any("offset", obj.Offset),
			slog.Any("size", obj.Size),
			slog.Any("timeout", timeout),
		)
	}()

	endPoint, err := url.Parse(
		fmt.Sprintf("%s/cacheFile/%s", h.cdnUrl, obj.Id),
	)
	if err != nil {
		return nil, err
	}

	rawQuery := endPoint.Query()
	rawQuery.Set(
		"range",
		fmt.Sprintf("%d,%d", obj.Offset, obj.Size),
	)

	endPoint.RawQuery = rawQuery.Encode()
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		endPoint.String(),
		nil,
	)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "*/*")

	resp, err := h.handleRequest(
		ctx,
		req,
		http.StatusOK,
		time.Duration(timeout)*time.Second,
		true,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

func (h *CdnHelper) Delete(ctx context.Context, obj *Object) error {
	now := time.Now().UTC()
	defer func() {
		cdnLogger.DebugContext(
			ctx,
			"Cdn delete file info",
			slog.Any("run time", time.Since(now).Seconds()),
			slog.Any("file id", obj.Id),
		)
	}()

	req, err := http.NewRequest(
		http.MethodDelete,
		fmt.Sprintf("%s/endFileRecord?file_record_id=%s", h.cdnUrl, obj.Id),
		nil,
	)
	if err != nil {
		return err
	}

	resp, err := h.handleRequest(
		ctx,
		req,
		http.StatusOK,
		3*time.Second,
		true,
		nil,
	)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf(
			"cdn response with code: %d, file id: %v",
			resp.StatusCode,
			obj.Id,
		)
	}

	return nil
}

func (h *CdnHelper) GetLink(ctx context.Context, obj *Object) (
	string, int64, error,
) {
	now := time.Now().UTC()
	defer func() {
		cdnLogger.DebugContext(
			ctx,
			"Cdn get link file info",
			slog.Any("run time", time.Since(now).Seconds()),
			slog.Any("file id", obj.Id),
			slog.Any("offset", obj.Offset),
			slog.Any("size", obj.Size),
		)
	}()

	tickerKey := fmt.Sprintf("%s-%d-%d", obj.Id, obj.Offset, obj.Size)
	dt, ok := h.ticketMapping.Load(tickerKey)
	if ok {
		lastTicket, ok := dt.(GetTicketResponse)
		if ok && lastTicket.ExpiredAt-1000000 > time.Now().UTC().UnixNano() {
			return fmt.Sprintf(
				"%s/file/%s?expire=%d&signature=%s&range=%d,%d",
				h.hubUrl,
				obj.Id,
				lastTicket.ExpiredAt,
				lastTicket.DecodedSignature,
				obj.Offset,
				obj.Size,
			), lastTicket.ExpiredAt - time.Second.Nanoseconds(), nil
		}
	}

	canGenerate, err := h.canGeneratePresignedLink(ctx, obj.Id)
	if err != nil {
		return "", 0, err
	}

	if !canGenerate {
		return "", 0, nil
	}

	ticket, err := h.getFileRecordTicket(
		ctx,
		obj.Id,
		fmt.Sprintf("%d,%d", obj.Offset, obj.Size),
	)
	if err != nil {
		return "", 0, err
	}

	h.ticketMapping.Store(tickerKey, *ticket)
	return fmt.Sprintf(
		"%s/file/%s?expire=%d&signature=%s&range=%s",
		h.hubUrl,
		obj.Id,
		ticket.ExpiredAt,
		ticket.DecodedSignature,
		fmt.Sprintf("%d,%d", obj.Offset, obj.Size),
	), ticket.ExpiredAt - time.Second.Nanoseconds(), nil
}

func (c *CdnHelper) GetFileRecord(
	ctx context.Context,
	fileId string,
) (*GetFileRecordResponse, error) {
	now := time.Now().UTC()
	defer func() {
		cdnLogger.DebugContext(
			ctx,
			"Cdn get file record",
			slog.Any("run time", time.Since(now).Seconds()),
			slog.Any("file id", fileId),
		)
	}()

	req, err := http.NewRequest(
		http.MethodGet,
		fmt.Sprintf("%s/getFileRecord/%s", c.cdnUrl, fileId),
		nil,
	)
	if err != nil {
		return nil, err
	}

	resp, err := c.handleRequest(
		ctx,
		req,
		http.StatusOK,
		3*time.Second,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	var rs GetFileRecordResponse
	if err := json.NewDecoder(resp.Body).Decode(&rs); err != nil {
		return nil, err
	}

	return &rs, nil
}

func (c *CdnHelper) GetZipHeader(ctx context.Context, fileId string) (*zipHeader, error) {
	now := time.Now().UTC()
	defer func() {
		cdnLogger.DebugContext(
			ctx,
			"Cdn get zip header",
			slog.Any("run time", time.Since(now).Seconds()),
			slog.Any("file id", fileId),
		)
	}()

	req, err := http.NewRequest(
		http.MethodGet,
		fmt.Sprintf("%s/getZipHeaders/%s", c.cdnUrl, fileId),
		nil,
	)
	if err != nil {
		return nil, err
	}

	resp, err := c.handleRequest(
		ctx,
		req,
		http.StatusOK,
		3*time.Second,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	var rs zipHeader
	if err := json.NewDecoder(resp.Body).Decode(&rs); err != nil {
		return nil, err
	}

	return &rs, nil
}

func (h *CdnHelper) GetTranscodeStatus(ctx context.Context, fileId string) (string, error) {
	now := time.Now().UTC()
	defer func() {
		cdnLogger.DebugContext(
			ctx,
			"Cdn get transcode status info",
			slog.Any("run time", time.Since(now).Seconds()),
			slog.Any("file id", fileId),
		)
	}()

	resp, err := h.GetFileRecord(ctx, fileId)
	if err != nil {
		return "", err
	}

	switch resp.Status {
	case 1:
		return TranscodingStatus, nil
	case 2:
		return TranscodedStatus, nil
	default:
		return FailStatus, nil
	}
}

func (h *CdnHelper) canGeneratePresignedLink(ctx context.Context, fileId string) (
	bool, error,
) {
	resp, err := h.GetFileRecord(ctx, fileId)
	if err != nil {
		return false, err
	}

	return resp.Status == FileRecordReadyStatus, nil
}

func (h *CdnHelper) getFileRecordTicket(
	ctx context.Context, fileId string,
	fileRange string,
) (*GetTicketResponse, error) {
	var data GetTicketResponse
	req, err := http.NewRequest(
		http.MethodGet,
		fmt.Sprintf(
			"%s/getTicket?id=%s&range=%s",
			h.cdnUrl,
			fileId,
			fileRange,
		),
		nil,
	)
	if err != nil {
		return nil, err
	}

	resp, err := h.handleRequest(
		ctx,
		req,
		http.StatusOK,
		3*time.Second,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, nil
	}

	decodeSignature, err := base64.StdEncoding.DecodeString(data.Signature)
	if err != nil {
		return nil, err
	}

	data.DecodedSignature = base64.RawURLEncoding.EncodeToString(decodeSignature)

	return &data, nil
}

func (h *CdnHelper) getBalance() error {
	req, err := http.NewRequest(
		http.MethodGet,
		fmt.Sprintf("%s/getBalance", h.cdnUrl),
		nil,
	)
	if err != nil {
		return err
	}

	resp, err := h.handleRequest(
		context.Background(),
		req,
		http.StatusOK,
		3*time.Second,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	var data GetBalanceResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return err
	}

	if data.DepositAddress != h.businessAddress {
		return fmt.Errorf(
			"business address is not match, cdn address: %s, business address: %s",
			data.DepositAddress,
			h.businessAddress,
		)
	}

	if !data.SetCreditLater {
		return fmt.Errorf("cdn not set credit later")
	}

	return nil
}

func (h *CdnHelper) GetAIOZPrice(ctx context.Context) (
	float64, error,
) {
	now := time.Now().UTC()
	defer func() {
		cdnLogger.DebugContext(
			ctx,
			"Cdn get aioz price info",
			slog.Any("run time", time.Since(now).Seconds()),
		)
	}()

	req, err := http.NewRequest(
		http.MethodGet,
		fmt.Sprintf("%s/currentPrice", h.cdnUrl),
		nil,
	)
	if err != nil {
		return 0, err
	}

	resp, err := h.handleRequest(
		ctx,
		req,
		http.StatusOK,
		3*time.Second,
		true,
		nil,
	)
	if err != nil {
		return 0, err
	}

	defer resp.Body.Close()
	var getPriceResp GetAiozPriceResponse
	if err := json.NewDecoder(resp.Body).Decode(&getPriceResp); err != nil {
		return 0, err
	}

	rs, err := strconv.ParseFloat(getPriceResp.AiozPrice, 64)
	if err != nil {
		return 0, err
	}

	return rs, nil
}

func (h *CdnHelper) GetDetailBalance(ctx context.Context) (*GetDetailBalanceResponse, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		fmt.Sprintf("%s/getBalance", h.cdnUrl),
		nil,
	)
	if err != nil {
		return nil, err
	}

	resp, err := h.handleRequest(
		context.Background(),
		req,
		http.StatusOK,
		3*time.Second,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	var data GetDetailBalanceResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	if data.DepositAddress != h.businessAddress {
		return nil, fmt.Errorf(
			"business address is not match, cdn address: %s, business address: %s",
			data.DepositAddress,
			h.businessAddress,
		)
	}

	if !data.SetCreditLater {
		return nil, fmt.Errorf("cdn not set credit later")
	}

	return &data, nil
}

// ============================================================================
// MediaMTX StorageProvider Wrapper
// ============================================================================

// CDNStorageProvider implements StorageProvider, ReadableStorageProvider, and LinkStorageProvider
// by wrapping the CdnHelper.
type CDNStorageProvider struct {
	helper   *CdnHelper
	keyToObj sync.Map
}

func NewCDNStorageProvider(cdnURL, hubURL, businessAddress string, options ...Option) (*CDNStorageProvider, error) {
	if strings.TrimSpace(cdnURL) == "" {
		return nil, fmt.Errorf("cdn: endpoint url is required")
	}
	if strings.TrimSpace(hubURL) == "" {
		return nil, fmt.Errorf("cdn: hub url is required")
	}

	helper := MustNewCdnHelper(cdnURL, hubURL, businessAddress, options...)
	return &CDNStorageProvider{
		helper: helper,
	}, nil
}

func (p *CDNStorageProvider) Helper() *CdnHelper {
	return p.helper
}

func (p *CDNStorageProvider) Name() string {
	return string(ProviderCDN)
}

func (p *CDNStorageProvider) CDNURL() string {
	return p.helper.cdnUrl
}

func (p *CDNStorageProvider) HubURL() string {
	return p.helper.hubUrl
}

func (p *CDNStorageProvider) BusinessAddress() string {
	return p.helper.businessAddress
}

func (p *CDNStorageProvider) RegisterKeyFileInfo(key, fileID string, offset, size int64) {
	if key != "" && fileID != "" {
		p.keyToObj.Store(key, &Object{
			Id:     fileID,
			Offset: offset,
			Size:   size,
			Name:   key,
		})
	}
}

func (p *CDNStorageProvider) RegisterKeyETag(key, etag string) {
	if key == "" || etag == "" {
		return
	}
	if obj, ok := parseCDNETag(etag); ok {
		obj.Name = key
		p.keyToObj.Store(key, obj)
	}
}

func parseCDNETag(etag string) (*Object, bool) {
	parts := strings.Split(etag, ":")
	if len(parts) == 3 && parts[0] != "" {
		offset, err1 := strconv.ParseInt(parts[1], 10, 64)
		size, err2 := strconv.ParseInt(parts[2], 10, 64)
		if err1 == nil && err2 == nil {
			return &Object{
				Id:     parts[0],
				Offset: offset,
				Size:   size,
			}, true
		}
	}
	return nil, false
}

func (p *CDNStorageProvider) getObject(key string) (*Object, bool) {
	if val, ok := p.keyToObj.Load(key); ok {
		if obj, ok := val.(*Object); ok {
			return obj, true
		}
	}
	return parseCDNETag(key)
}

func (p *CDNStorageProvider) UploadFile(ctx context.Context, localPath, remoteKey, contentType string) (string, error) {
	data, err := os.ReadFile(localPath)
	if err != nil {
		return "", fmt.Errorf("cdn: read local file %s: %w", localPath, err)
	}

	obj, err := p.helper.PackUploadByte(ctx, path.Base(remoteKey), data)
	if err != nil {
		return "", fmt.Errorf("cdn: upload %s: %w", remoteKey, err)
	}

	etag := fmt.Sprintf("%s:%d:%d", obj.Id, obj.Offset, obj.Size)
	p.keyToObj.Store(remoteKey, obj)
	p.keyToObj.Store(etag, obj)
	return etag, nil
}

func (p *CDNStorageProvider) GetLink(ctx context.Context, key string) (string, error) {
	obj, ok := p.getObject(key)
	if !ok {
		return "", fmt.Errorf("cdn: unknown file info for key %q", key)
	}

	link, _, err := p.helper.GetLink(ctx, obj)
	if err != nil {
		return "", fmt.Errorf("cdn: get link for %s: %w", key, err)
	}
	if link == "" {
		return "", fmt.Errorf("cdn: cannot generate presigned link for file %s (not ready)", obj.Id)
	}

	return link, nil
}

func (p *CDNStorageProvider) GetObject(ctx context.Context, key string) (io.ReadCloser, string, int64, error) {
	obj, ok := p.getObject(key)
	if !ok {
		return nil, "", -1, fmt.Errorf("cdn: unknown file info for key %q", key)
	}

	reader, err := p.helper.Download(ctx, obj)
	if err != nil {
		return nil, "", -1, fmt.Errorf("cdn: download %s: %w", key, err)
	}

	readCloser, ok := reader.(io.ReadCloser)
	if !ok {
		readCloser = io.NopCloser(reader)
	}

	contentType := contentTypeForRemoteKey(key)
	return readCloser, contentType, obj.Size, nil
}

func (p *CDNStorageProvider) DeleteFolder(ctx context.Context, prefix string) error {
	// CDN storage handles retention and lifecycle independently
	return nil
}

func (p *CDNStorageProvider) Close() error {
	return nil
}

func NewCDNStorageProviderFromConfig(_ context.Context, cfg StorageConfig) (StorageProvider, error) {
	cdnURL := getResolvedValue(cfg.CDNEndpoint, "MTX_CDNENDPOINT", "CDN_ENDPOINT", "CDN_URL", cfg.Endpoint)
	hubURL := getResolvedValue(cfg.CDNHubURL, "MTX_CDNHUBURL", "CDN_HUBURL", "CDN_HUB_URL", "")
	businessAddress := getResolvedValue(cfg.CDNBusinessAddress, "MTX_CDNBUSINESSADDRESS", "CDN_BUSINESSADDRESS", "CDN_BUSINESS_ADDRESS", "")

	return NewCDNStorageProvider(cdnURL, hubURL, businessAddress)
}

func init() {
	RegisterProvider(ProviderCDN, NewCDNStorageProviderFromConfig)
}
