package sources

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/torrin-app/torrin/internal/jobs"
	"github.com/torrin-app/torrin/internal/r2"
)

type File struct {
	Name     string                                           // release/display filename
	Size     int64                                            // bytes (0 if unknown up front)
	CacheKey string                                           // stable 40-hex dedup key (the infohash-equivalent)
	Source   string                                           // "telegram", ...
	Open     func(ctx context.Context) (io.ReadCloser, error) // streams the file bytes
}

type manifestFile struct {
	FileName  string `json:"file_name"`
	DirectURL string `json:"direct_url"`
	FileSize  int64  `json:"file_size"`
}

type manifest struct {
	Name  string         `json:"name"`
	Files []manifestFile `json:"files"`
}

func Ingest(ctx context.Context, r2c *r2.Client, store *jobs.Store, f File, userID string) (*jobs.Job, error) {
	if f.CacheKey == "" || f.Name == "" {
		return nil, fmt.Errorf("sources: file needs CacheKey and Name")
	}
	streamPath := f.CacheKey + "/file_0/" + f.Name

	cached, _ := r2c.HasManifest(ctx, f.CacheKey)
	if !cached {
		rc, err := f.Open(ctx)
		if err != nil {
			return nil, fmt.Errorf("sources: open %s: %w", f.Source, err)
		}
		defer rc.Close()

		if err := r2c.StreamUpload(ctx, streamPath, rc, "application/octet-stream"); err != nil {
			return nil, fmt.Errorf("sources: stream upload: %w", err)
		}
		man, _ := json.Marshal(manifest{
			Name:  f.Name,
			Files: []manifestFile{{FileName: f.Name, DirectURL: streamPath, FileSize: f.Size}},
		})
		if err := r2c.UploadFile(ctx, f.CacheKey+"/manifest.json", bytes.NewReader(man), "application/json"); err != nil {
			return nil, fmt.Errorf("sources: manifest upload: %w", err)
		}
	}

	job := &jobs.Job{
		ID:       newID(),
		UserID:   userID,
		InfoHash: f.CacheKey,
		Name:     f.Name,
		Source:   f.Source,
		Status:   jobs.StatusComplete,
		FileSize: f.Size,
		StreamURLs: []jobs.Stream{{
			FileName:  f.Name,
			Size:      f.Size,
			DirectURL: streamPath,
			SignedURL: r2c.SignURL(streamPath, 24*time.Hour),
		}},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := store.Create(job); err != nil {
		return nil, fmt.Errorf("sources: create job: %w", err)
	}
	return job, nil
}

func newID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}
