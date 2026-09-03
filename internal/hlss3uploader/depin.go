package hlss3uploader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	uplinksdk "aioz-depin/go-sdk"

	"go.uber.org/zap"
)

const (
	defaultDePINRegion    = "global"
	depinUploadTimeout    = 2 * time.Minute
	depinDownloadTimeout  = 30 * time.Second
	GetLinkExpiry         = 24 * time.Hour
	linkCacheSafetyMargin = time.Minute
)

type cachedGoSdkLink struct {
	link      string
	expiresAt int64 // unix nanoseconds
}

// DePINStorageProvider implements StorageProvider, ReadableStorageProvider, and LinkStorageProvider
// using the aioz-depin/go-sdk to store HLS livestream segments on the DePIN network.
type DePINStorageProvider struct {
	client       *uplinksdk.Client
	linkEndpoint string

	// keyToUUID maps remoteKey -> go-sdk file UUID string.
	// Populated on UploadFile, consumed by GetObject and GetLink.
	keyToUUID sync.Map

	// linkCache memoizes minted download links by file UUID.
	linkCache sync.Map

	// contractID is the mandatory storage contract for this stream.
	// Created once per stream during initialization, shared across all segment uploads of the stream.
	contractID string
}

func (p *DePINStorageProvider) Name() string {
	return string(ProviderDePIN)
}

func (p *DePINStorageProvider) ContractID() string {
	return p.contractID
}

func (p *DePINStorageProvider) LinkEndpoint() string {
	return p.linkEndpoint
}

func (p *DePINStorageProvider) RegisterKeyUUID(key, uuidStr string) {
	if key != "" && uuidStr != "" {
		p.keyToUUID.Store(key, uuidStr)
	}
}

func (p *DePINStorageProvider) GetLink(ctx context.Context, key string) (string, error) {
	var fileUUIDStr string
	if val, ok := p.keyToUUID.Load(key); ok {
		fileUUIDStr = val.(string)
	} else {
		fileUUIDStr = key
	}

	fileID, err := uplinksdk.ParseUUID(fileUUIDStr)
	if err != nil {
		return "", fmt.Errorf("depin: get link: parse uuid for key %q: %w", key, err)
	}

	cacheKey := fileID.String()
	if cached, ok := p.linkCache.Load(cacheKey); ok {
		if c, ok := cached.(cachedGoSdkLink); ok &&
			time.Until(time.Unix(0, c.expiresAt)) > linkCacheSafetyMargin {
			return c.link, nil
		}
	}

	if ctx == nil {
		ctx = context.Background()
	}

	expiresAt := time.Now().Add(GetLinkExpiry)
	ticket, err := p.client.CreateDownloadTicketLocal(fileID, expiresAt)
	expiry := expiresAt
	if err != nil {
		var netErr error
		ticket, expiry, netErr = p.client.CreateDownloadTicket(ctx, fileID, expiresAt)
		if netErr != nil {
			return "", fmt.Errorf("depin: get link: %w", netErr)
		}
	}

	endpoint := strings.TrimRight(p.linkEndpoint, "/")
	if endpoint == "" {
		endpoint = "http://localhost:8080"
	}
	link := fmt.Sprintf("%s/download/%s?ticket=%s", endpoint, fileID.String(), ticket)

	p.linkCache.Store(cacheKey, cachedGoSdkLink{
		link:      link,
		expiresAt: expiry.UnixNano(),
	})

	return link, nil
}

func (p *DePINStorageProvider) UploadFile(ctx context.Context, localPath, remoteKey, contentType string) (string, error) {
	if p.contractID == "" {
		return "", fmt.Errorf("depin: upload rejected: contract ID is mandatory but is empty")
	}

	contractUUID, err := uplinksdk.ParseUUID(p.contractID)
	if err != nil {
		return "", fmt.Errorf("depin: upload rejected: invalid contract ID %q: %w", p.contractID, err)
	}

	data, err := os.ReadFile(localPath)
	if err != nil {
		return "", fmt.Errorf("depin: read %s: %w", localPath, err)
	}

	uploadParams := uplinksdk.UploadParams{
		ContractID:    contractUUID,
		Filename:      remoteKey,
		Size:          int64(len(data)),
		SystemHeaders: map[string]string{"Content-Type": contentType},
	}

	uploadCtx, cancel := context.WithTimeout(ctx, depinUploadTimeout)
	defer cancel()

	fileID, _, err := p.client.UploadFile(uploadCtx,
		bytes.NewReader(data),
		uploadParams,
	)
	if err != nil {
		return "", fmt.Errorf("depin: upload %s under contract %s: %w", remoteKey, p.contractID, err)
	}

	uuidStr := fileID.String()
	p.keyToUUID.Store(remoteKey, uuidStr)
	return uuidStr, nil
}

func (p *DePINStorageProvider) GetObject(ctx context.Context, key string) (io.ReadCloser, string, int64, error) {
	uuidStr, ok := p.keyToUUID.Load(key)
	if !ok {
		return nil, "", -1, fmt.Errorf("depin: unknown key %s", key)
	}

	fileUUID, err := uplinksdk.ParseUUID(uuidStr.(string))
	if err != nil {
		return nil, "", -1, fmt.Errorf("depin: parse uuid: %w", err)
	}

	pr, pw := io.Pipe()
	go func() {
		downloadCtx, cancel := context.WithTimeout(ctx, depinDownloadTimeout)
		defer cancel()
		err := p.client.DownloadFile(downloadCtx, fileUUID, pw)
		_ = pw.CloseWithError(err)
	}()

	contentType := contentTypeForRemoteKey(key)
	return pr, contentType, -1, nil
}

func (p *DePINStorageProvider) DeleteFolder(ctx context.Context, prefix string) error {
	if p.contractID == "" {
		return nil
	}
	contractUUID, err := uplinksdk.ParseUUID(p.contractID)
	if err != nil {
		return fmt.Errorf("depin: parse contract id: %w", err)
	}
	_, err = p.client.DeleteContract(ctx, contractUUID, true)
	if err != nil {
		return fmt.Errorf("depin: delete contract: %w", err)
	}
	return nil
}

func (p *DePINStorageProvider) Close() error {
	if p.client != nil {
		return p.client.Close()
	}
	return nil
}

func init() {
	RegisterProvider(ProviderDePIN, func(ctx context.Context, cfg StorageConfig) (StorageProvider, error) {
		identityDir := getResolvedValue(cfg.DePINIdentityDir, "")
		coordPeerURL := getResolvedValue(cfg.DePINCoordPeerURL, "")
		pieceKeyPath := getResolvedValue(cfg.DePINPieceKeyPath, "")
		linkEndpoint := getResolvedValue(cfg.DePINLinkEndpoint, "")

		if identityDir == "" || coordPeerURL == "" {
			return nil, fmt.Errorf("depin: identity_dir and coord_peer_url are required")
		}

		zapLogger, err := zap.NewProduction()
		if err != nil {
			zapLogger = zap.NewNop()
		}

		opts := []uplinksdk.ClientOption{
			uplinksdk.WithIdentityDir(identityDir),
			uplinksdk.WithCoordPeerURL(coordPeerURL),
			uplinksdk.WithLogger(zapLogger),
		}
		if pieceKeyPath != "" {
			opts = append(opts, uplinksdk.WithPieceKey(pieceKeyPath))
		}

		client, err := uplinksdk.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("depin: init client: %w", err)
		}

		region := defaultDePINRegion
		if strings.TrimSpace(cfg.Region) != "" {
			region = strings.TrimSpace(cfg.Region)
		}

		placements, err := client.ListPlacements(ctx)
		if err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("depin: list placements: %w", err)
		}
		if len(placements) == 0 {
			_ = client.Close()
			return nil, fmt.Errorf("depin: coordinator returned no placements")
		}

		chosenPlacement := placements[0].PlacementId
		tags := map[string]string{
			"service": "mediamtx-hls",
			"type":    "livestream",
		}
		if cfg.StreamName != "" {
			tags["stream_name"] = cfg.StreamName
		} else {
			tags["role"] = "dvr"
		}

		// Ensure client identity is registered with the coordinator if not already present
		if _, err := client.GetAccount(ctx); err != nil {
			email := fmt.Sprintf("mediamtx+%s@aioz.network", client.Identity().ID.String())
			_, _ = client.RegisterAccount(ctx, email)
		}

		cID, err := client.CreateContractWithPlacement(
			ctx,
			region,
			chosenPlacement,
			tags,
		)
		if err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("depin: create contract for stream %q (placement %d): %w", cfg.StreamName, chosenPlacement, err)
		}

		return &DePINStorageProvider{
			client:       client,
			linkEndpoint: linkEndpoint,
			contractID:   cID.String(),
		}, nil
	})
}
