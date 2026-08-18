package hlss3uploader

import (
	"bytes"
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

	paginator := s3.NewListObjectsV2Paginator(p.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(p.bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}

		var objects []types.ObjectIdentifier
		for _, obj := range page.Contents {
			objects = append(objects, types.ObjectIdentifier{Key: obj.Key})
		}

		if len(objects) > 0 {
			_, err = p.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(p.bucket),
				Delete: &types.Delete{
					Objects: objects,
					Quiet:   aws.Bool(true),
				},
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}
