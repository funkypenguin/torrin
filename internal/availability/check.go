package availability

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/torrin-app/torrin/internal/r2"
)

type Result struct {
	Available bool       `json:"available"`
	InfoHash  string     `json:"info_hash"`
	Name      string     `json:"name,omitempty"`
	Files     []FileInfo `json:"files,omitempty"`
}

type FileInfo struct {
	FileName string `json:"file_name"`
	URL      string `json:"url"`
	Size     int64  `json:"size"`
}

func Check(ctx context.Context, r2Client *r2.Client, infoHash string) (*Result, error) {
	infoHash = strings.ToLower(infoHash)

	has, err := r2Client.HasManifest(ctx, infoHash)
	if err != nil {
		return &Result{Available: false, InfoHash: infoHash}, nil
	}
	if !has {
		return &Result{Available: false, InfoHash: infoHash}, nil
	}

	data, err := r2Client.GetManifest(ctx, infoHash)
	if err != nil {
		return &Result{Available: false, InfoHash: infoHash}, nil
	}

	var manifest struct {
		Name  string `json:"name"`
		Files []struct {
			FileName  string `json:"file_name"`
			DirectURL string `json:"direct_url"`
			FileSize  int64  `json:"file_size"`
		} `json:"files"`
	}
	json.Unmarshal(data, &manifest)

	files := make([]FileInfo, len(manifest.Files))
	for i, f := range manifest.Files {
		files[i] = FileInfo{
			FileName: f.FileName,
			URL:      f.DirectURL,
			Size:     f.FileSize,
		}
	}

	return &Result{
		Available: true,
		InfoHash:  infoHash,
		Name:      manifest.Name,
		Files:     files,
	}, nil
}

func CheckBatch(ctx context.Context, r2Client *r2.Client, hashes []string) []Result {
	results := make([]Result, len(hashes))
	for i, h := range hashes {
		r, _ := Check(ctx, r2Client, h)
		if r != nil {
			results[i] = *r
		} else {
			results[i] = Result{Available: false, InfoHash: h}
		}
	}
	return results
}
