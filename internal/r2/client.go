package r2

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Client struct {
	s3         *s3.Client
	bucket     string
	publicURL  string
	signingKey []byte
}

func NewClient(accountID, accessKey, secretKey, bucket, publicURL, signingKey string) *Client {
	endpoint := fmt.Sprintf("https://%s.eu.r2.cloudflarestorage.com", accountID)

	s3Client := s3.New(s3.Options{
		Region:       "auto",
		BaseEndpoint: &endpoint,
		Credentials:  credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
	})

	return &Client{
		s3:         s3Client,
		bucket:     bucket,
		publicURL:  strings.TrimRight(publicURL, "/"),
		signingKey: []byte(signingKey),
	}
}

func (c *Client) HasManifest(ctx context.Context, infoHash string) (bool, error) {
	key := fmt.Sprintf("%s/manifest.json", infoHash)
	_, err := c.s3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &c.bucket,
		Key:    &key,
	})
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "404") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *Client) GetManifest(ctx context.Context, infoHash string) ([]byte, error) {
	key := fmt.Sprintf("%s/manifest.json", infoHash)
	out, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &c.bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

func (c *Client) UploadFile(ctx context.Context, key string, reader io.Reader, contentType string) error {
	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &c.bucket,
		Key:         &key,
		Body:        reader,
		ContentType: &contentType,
	})
	return err
}

func (c *Client) StreamUpload(ctx context.Context, key string, reader io.Reader, contentType string) error {
	uploader := manager.NewUploader(c.s3, func(u *manager.Uploader) {
		u.PartSize = 32 * 1024 * 1024
		u.Concurrency = 8
		u.LeavePartsOnError = false
	})

	_, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:      &c.bucket,
		Key:         &key,
		Body:        reader,
		ContentType: &contentType,
	})
	return err
}

func (c *Client) DeletePrefix(ctx context.Context, prefix string) error {
	list, err := c.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: &c.bucket,
		Prefix: &prefix,
	})
	if err != nil {
		return err
	}
	for _, obj := range list.Contents {
		c.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: &c.bucket,
			Key:    obj.Key,
		})
	}
	return nil
}

func (c *Client) SignURL(path string, expiry time.Duration) string {
	expires := time.Now().Add(expiry).Unix()
	msg := fmt.Sprintf("%s:%d", path, expires)
	mac := hmac.New(sha256.New, c.signingKey)
	mac.Write([]byte(msg))
	sig := hex.EncodeToString(mac.Sum(nil))
	parts := strings.Split(path, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	encodedPath := strings.Join(parts, "/")
	return fmt.Sprintf("%s/%s?expires=%d&sig=%s", c.publicURL, encodedPath, expires, sig)
}

func contentTypeFor(name string) string {
	switch {
	case strings.HasSuffix(name, ".m3u8"):
		return "application/vnd.apple.mpegurl"
	case strings.HasSuffix(name, ".ts"):
		return "video/mp2t"
	case strings.HasSuffix(name, ".json"):
		return "application/json"
	case strings.HasSuffix(name, ".mp4"):
		return "video/mp4"
	default:
		return "application/octet-stream"
	}
}

func (c *Client) PublicURL() string {
	return c.publicURL
}

func (c *Client) SigningKey() []byte {
	return c.signingKey
}

func (c *Client) BucketName() string {
	return c.bucket
}

func (c *Client) S3() *s3.Client {
	return c.s3
}
