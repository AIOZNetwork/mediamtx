package hlss3uploader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3StorageProvider implements StorageProvider for S3 / S3-compatible storage.
type S3StorageProvider struct {
	client        *s3.Client
	presignClient *s3.PresignClient
	bucket        string
}

// NewS3StorageProvider initializes an S3 storage provider.
func NewS3StorageProvider(endpoint, bucket, region, accessKey, secretKey string) (*S3StorageProvider, error) {
	if bucket == "" || endpoint == "" {
		return nil, fmt.Errorf("S3 bucket or endpoint not set")
	}

	resolver := aws.EndpointResolverWithOptionsFunc(func(service, reg string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL:           endpoint,
			SigningRegion: region,
		}, nil
	})

	credentials := aws.CredentialsProviderFunc(func(c context.Context) (aws.Credentials, error) {
		return aws.Credentials{
			AccessKeyID:     accessKey,
			SecretAccessKey: secretKey,
		}, nil
	})

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithCredentialsProvider(credentials),
		config.WithEndpointResolverWithOptions(resolver),
		config.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
	)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	return &S3StorageProvider{
		client:        client,
		presignClient: s3.NewPresignClient(client),
		bucket:        bucket,
	}, nil
}

func (p *S3StorageProvider) Name() string {
	return string(ProviderS3)
}

func (p *S3StorageProvider) Bucket() string {
	return p.bucket
}

func (p *S3StorageProvider) PresignClient() *s3.PresignClient {
	return p.presignClient
}

func (p *S3StorageProvider) GetLink(ctx context.Context, key string) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := p.presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(defaultPresignExpires))
	if err != nil {
		return "", err
	}
	return req.URL, nil
}

func (p *S3StorageProvider) UploadFile(ctx context.Context, localPath, remoteKey, contentType string) (string, error) {
	byts, err := os.ReadFile(localPath)
	if err != nil {
		return "", err
	}

	out, err := p.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(p.bucket),
		Key:           aws.String(remoteKey),
		Body:          bytes.NewReader(byts),
		ContentLength: aws.Int64(int64(len(byts))),
		ContentType:   aws.String(contentType),
	})
	if err != nil {
		return "", err
	}
	if out.ETag != nil {
		return *out.ETag, nil
	}
	return "", nil
}

func (p *S3StorageProvider) Close() error {
	return nil
}

func (p *S3StorageProvider) GetObject(ctx context.Context, key string) (io.ReadCloser, string, int64, error) {
	out, err := p.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, "", 0, err
	}
	contentType := ""
	if out.ContentType != nil {
		contentType = *out.ContentType
	}
	contentLength := int64(-1)
	if out.ContentLength != nil {
		contentLength = *out.ContentLength
	}
	return out.Body, contentType, contentLength, nil
}

func init() {
	RegisterProvider(ProviderS3, func(_ context.Context, cfg StorageConfig) (StorageProvider, error) {
		endpoint := getResolvedValue(cfg.Endpoint)
		bucket := getResolvedValue(cfg.Bucket)
		region := getResolvedValue(cfg.Region)
		accessKey := getResolvedValue(cfg.AccessKeyID)
		secretKey := getResolvedValue(cfg.SecretAccessKey)

		return NewS3StorageProvider(endpoint, bucket, region, accessKey, secretKey)
	})
}

func (p *S3StorageProvider) DeleteFolder(ctx context.Context, prefix string) error {
	// Append trailing slash to ensure it acts as a folder
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}

	// List all objects under the prefix
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(p.bucket),
		Prefix: aws.String(prefix),
	}

	deleted := 0
	for {
		output, err := p.client.ListObjectsV2(ctx, input)
		if err != nil {
			return fmt.Errorf("list objects under %s: %w", prefix, err)
		}

		// Delete each object individually (avoids Content-MD5 requirement of batch DeleteObjects)
		for _, obj := range output.Contents {
			_, err := p.client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(p.bucket),
				Key:    obj.Key,
			})
			if err != nil {
				return fmt.Errorf("delete object %s: %w", *obj.Key, err)
			}
			deleted++
		}

		if !aws.ToBool(output.IsTruncated) {
			break
		}
		input.ContinuationToken = output.NextContinuationToken
	}

	if deleted == 0 {
		return fmt.Errorf("no objects found under prefix %s", prefix)
	}
	return nil
}
