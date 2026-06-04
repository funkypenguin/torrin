package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/torrin-app/torrin/internal/jobs"
	"github.com/torrin-app/torrin/internal/qbit"
	"github.com/torrin-app/torrin/internal/usenet"
)

func (p *Poller) uploadAndFinalize(ctx context.Context, job *jobs.Job, t *qbit.Torrent) {
	files, err := p.qb.GetFiles(t.Hash)
	if err != nil {
		slog.Error("get files for upload", "err", err)
		return
	}

	var streamURLs []jobs.Stream
	var uploadedSize int64

	for i, f := range files {
		if f.Priority == 0 || f.Size == 0 {
			continue
		}
		if !isVideoFile(f.Name) {
			continue
		}

		baseName := filepath.Base(f.Name)
		safeBaseName := strings.ReplaceAll(baseName, " ", "_")
		r2Key := fmt.Sprintf("%s/file_%d/%s", job.InfoHash, i, safeBaseName)

		localPath := filepath.Join(t.SavePath, f.Name)

		slog.Info("uploading to R2", "job", job.ID, "file", baseName, "path", localPath)

		file, err := os.Open(localPath)
		if err != nil {
			slog.Error("open file", "path", localPath, "err", err)
			job.Status = jobs.StatusFailed
			job.Error = fmt.Sprintf("open: %v", err)
			p.store.Update(job)
			return
		}

		ct := contentTypeFor(filepath.Ext(baseName))
		if err := p.r2.StreamUpload(ctx, r2Key, file, ct); err != nil {
			file.Close()
			slog.Error("upload to R2", "key", r2Key, "err", err)
			job.Status = jobs.StatusFailed
			job.Error = fmt.Sprintf("upload: %v", err)
			p.store.Update(job)
			return
		}
		file.Close()
		uploadedSize += f.Size

		slog.Info("uploaded", "key", r2Key, "size_mb", f.Size/1e6)

		streamURLs = append(streamURLs, jobs.Stream{
			FileName:  baseName,
			Size:      f.Size,
			DirectURL: r2Key,
			SignedURL: p.r2.SignURL(r2Key, 24*time.Hour),
		})
	}

	if len(streamURLs) == 0 {
		job.Status = jobs.StatusFailed
		job.Error = "no video files found"
		p.store.Update(job)
		p.deleteAndVerify(t.Hash, t)
		p.ReleaseFor(job.InfoHash)
		return
	}

	type manifestFile struct {
		FileName  string `json:"file_name"`
		DirectURL string `json:"direct_url"`
		FileSize  int64  `json:"file_size"`
	}
	var mFiles []manifestFile
	for _, s := range streamURLs {
		var sz int64
		for _, f := range files {
			if filepath.Base(f.Name) == s.FileName || strings.ReplaceAll(filepath.Base(f.Name), " ", "_") == s.FileName {
				sz = f.Size
				break
			}
		}
		mFiles = append(mFiles, manifestFile{FileName: s.FileName, DirectURL: s.DirectURL, FileSize: sz})
	}
	manifest := map[string]any{
		"info_hash": job.InfoHash, "name": job.Name,
		"files": mFiles, "created_at": time.Now(),
	}
	manifestJSON, _ := json.MarshalIndent(manifest, "", "  ")
	manifestKey := job.InfoHash + "/manifest.json"
	p.r2.UploadFile(ctx, manifestKey, strings.NewReader(string(manifestJSON)), "application/json")

	siblings, _ := p.store.ListByInfoHash(job.InfoHash)
	for _, sib := range siblings {
		sib.StreamURLs = streamURLs
		sib.Name = job.Name
		sib.Status = jobs.StatusComplete
		sib.Error = ""
		p.store.Update(sib)
		p.store.SetFileSize(sib.ID, uploadedSize)
	}

	p.deleteAndVerify(t.Hash, t)
	p.ReleaseFor(job.InfoHash)

	slog.Info("job complete", "job", job.ID, "name", job.Name, "streams", len(streamURLs), "users", len(siblings))
}

type localFile struct {
	Name string
	Path string
	Size int64
}

func (p *Poller) uploadLocalFiles(ctx context.Context, job *jobs.Job, files []usenet.OutputFile) {
	var streamURLs []jobs.Stream
	var uploadedSize int64

	for i, f := range files {
		if !isVideoFile(f.Name) {
			continue
		}

		safeBaseName := strings.ReplaceAll(f.Name, " ", "_")
		r2Key := fmt.Sprintf("%s/file_%d/%s", job.InfoHash, i, safeBaseName)

		slog.Info("uploading to R2", "job", job.ID, "file", f.Name, "path", f.Path)

		file, err := os.Open(f.Path)
		if err != nil {
			slog.Error("open file", "path", f.Path, "err", err)
			job.Status = jobs.StatusFailed
			job.Error = fmt.Sprintf("open: %v", err)
			p.store.Update(job)
			return
		}

		ct := contentTypeFor(filepath.Ext(f.Name))
		if err := p.r2.StreamUpload(ctx, r2Key, file, ct); err != nil {
			file.Close()
			slog.Error("upload to R2", "key", r2Key, "err", err)
			job.Status = jobs.StatusFailed
			job.Error = fmt.Sprintf("upload: %v", err)
			p.store.Update(job)
			return
		}
		file.Close()
		uploadedSize += f.Size

		slog.Info("uploaded", "key", r2Key, "size_mb", f.Size/1e6)

		streamURLs = append(streamURLs, jobs.Stream{
			FileName:  f.Name,
			Size:      f.Size,
			DirectURL: r2Key,
			SignedURL: p.r2.SignURL(r2Key, 24*time.Hour),
		})
	}

	if len(streamURLs) == 0 {
		job.Status = jobs.StatusFailed
		job.Error = "no video files found"
		p.store.Update(job)
		return
	}

	type manifestFile struct {
		FileName  string `json:"file_name"`
		DirectURL string `json:"direct_url"`
		FileSize  int64  `json:"file_size"`
	}
	var mFiles []manifestFile
	for _, s := range streamURLs {
		var sz int64
		for _, f := range files {
			if f.Name == s.FileName || strings.ReplaceAll(f.Name, " ", "_") == s.FileName {
				sz = f.Size
				break
			}
		}
		mFiles = append(mFiles, manifestFile{FileName: s.FileName, DirectURL: s.DirectURL, FileSize: sz})
	}
	manifest := map[string]any{
		"info_hash": job.InfoHash, "name": job.Name,
		"files": mFiles, "created_at": time.Now(),
	}
	manifestJSON, _ := json.MarshalIndent(manifest, "", "  ")
	manifestKey := job.InfoHash + "/manifest.json"
	p.r2.UploadFile(ctx, manifestKey, strings.NewReader(string(manifestJSON)), "application/json")

	siblings, _ := p.store.ListByInfoHash(job.InfoHash)
	for _, sib := range siblings {
		sib.StreamURLs = streamURLs
		sib.Name = job.Name
		sib.Status = jobs.StatusComplete
		sib.Error = ""
		p.store.Update(sib)
		p.store.SetFileSize(sib.ID, uploadedSize)
	}

	slog.Info("job complete", "job", job.ID, "name", job.Name, "streams", len(streamURLs), "users", len(siblings))
}
