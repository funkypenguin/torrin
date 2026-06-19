package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/torrin-app/torrin/internal/jobs"
	"github.com/torrin-app/torrin/internal/premiumize"
)

func (p *Poller) tryPremiumize(ctx context.Context, job *jobs.Job) bool {
	if p.pmKeyProvider == nil || job.Magnet == "" {
		return false
	}

	apiKey := p.pmKeyProvider(job.UserID)
	if apiKey == "" {
		return false
	}

	if _, checking := p.uploading.Load("pm-check-" + job.InfoHash); checking {
		return false
	}
	p.uploading.Store("pm-check-"+job.InfoHash, true)
	defer p.uploading.Delete("pm-check-" + job.InfoHash)

	log := slog.With("job", job.ID, "hash", job.InfoHash, "source", "premiumize")

	client := premiumize.NewClient(apiKey)

	// Check cache.
	cached, err := client.CheckCache(ctx, []string{job.Magnet})
	if err != nil {
		log.Debug("pm cache check failed", "err", err)
		return false
	}
	if len(cached) == 0 || !cached[0] {
		log.Debug("not cached on premiumize")
		return false
	}

	// Get direct download links.
	dl, err := client.DirectDL(ctx, job.Magnet)
	if err != nil {
		log.Warn("pm directdl failed", "err", err)
		return false
	}

	// Filter video files.
	var videoFiles []premiumize.DirectDLContent
	var totalVideoSize int64
	for _, f := range dl.Content {
		if isVideoFile(f.Path) {
			videoFiles = append(videoFiles, f)
			totalVideoSize += f.Size
		}
	}

	if len(videoFiles) == 0 {
		log.Info("pm: no video files, falling through")
		return false
	}

	// Size check.
	if job.MaxBytes > 0 && totalVideoSize > job.MaxBytes {
		log.Warn("pm torrent exceeds plan limit", "size_gb", totalVideoSize/1e9, "max_gb", job.MaxBytes/1e9)
		job.Status = jobs.StatusFailed
		job.Error = fmt.Sprintf("torrent size %dGB exceeds your plan limit of %dGB", totalVideoSize/1e9, job.MaxBytes/1e9)
		p.store.Update(job)
		return true
	}

	if _, already := p.uploading.LoadOrStore(job.InfoHash, true); already {
		return true
	}

	job.Status = jobs.StatusProcessing
	job.Error = "downloading"
	if job.Name == "" {
		job.Name = filepath.Base(videoFiles[0].Path)
	}
	job.FileSize = totalVideoSize
	p.store.SetFileSize(job.ID, totalVideoSize)
	p.store.Update(job)

	log.Info("content cached on premiumize", "files", len(videoFiles), "size_mb", totalVideoSize/1_000_000)

	p.UploadWg.Add(1)
	go func() {
		defer p.UploadWg.Done()
		select {
		case p.uploadSem <- struct{}{}:
		case <-ctx.Done():
			p.uploading.Delete(job.InfoHash)
			return
		}
		defer func() { <-p.uploadSem }()
		defer p.uploading.Delete(job.InfoHash)
		defer func() {
			if r := recover(); r != nil {
				slog.Error("panic in PM download", "err", r, "job", job.ID)
				job.Status = jobs.StatusFailed
				job.Error = "internal error"
				p.store.Update(job)
			}
		}()

		tmpDir := filepath.Join(p.rdDownloadDir, job.InfoHash)
		os.MkdirAll(tmpDir, 0755)
		defer os.RemoveAll(tmpDir)

		var downloadedFiles []downloadedFile

		for i, vf := range videoFiles {
			if ctx.Err() != nil {
				return
			}

			name := filepath.Base(vf.Path)
			localPath := filepath.Join(tmpDir, name)

			log.Info("downloading from pm", "file", name, "size_mb", vf.Size/1_000_000)

			if err := p.downloadFromURL(ctx, client, vf.Link, localPath, job, vf.Size, i, len(videoFiles)); err != nil {
				log.Error("pm download failed", "file", name, "err", err)
				p.failDownload(ctx, job, "download failed")
				return
			}

			info, _ := os.Stat(localPath)
			var size int64
			if info != nil {
				size = info.Size()
			}
			downloadedFiles = append(downloadedFiles, downloadedFile{Name: name, Path: localPath, Size: size})
		}

		if len(downloadedFiles) == 0 {
			job.Status = jobs.StatusFailed
			job.Error = "no video files downloaded"
			p.store.Update(job)
			return
		}

		p.uploadAndFinalizeFiles(ctx, job, downloadedFiles, log)
	}()

	return true
}

func (p *Poller) downloadFromURL(ctx context.Context, client interface {
	DownloadFileRange(context.Context, string, int64) (io.ReadCloser, int64, bool, error)
}, downloadURL, localPath string, job *jobs.Job, totalSize int64, fileIdx, fileCount int) error {
	open := func(offset int64) (io.ReadCloser, int64, bool, error) {
		return client.DownloadFileRange(ctx, downloadURL, offset)
	}
	return p.downloadToFile(ctx, open, localPath, job, totalSize, fileIdx, fileCount)
}

const minVideoFileSize = 1_000_000

func (p *Poller) uploadAndFinalizeFiles(ctx context.Context, job *jobs.Job, files []downloadedFile, log *slog.Logger) {
	// Validate file sizes before uploading.
	for _, f := range files {
		if f.Size < minVideoFileSize {
			log.Error("file too small, likely corrupted", "file", f.Name, "size", f.Size)
			job.Status = jobs.StatusFailed
			job.Error = "download corrupted (file too small)"
			p.store.Update(job)
			return
		}
	}

	job.Error = "uploading to cache"
	p.store.Update(job)

	var streamURLs []jobs.Stream
	var uploadedSize int64

	for i, f := range files {
		safeBaseName := strings.ReplaceAll(f.Name, " ", "_")
		r2Key := fmt.Sprintf("%s/file_%d/%s", job.InfoHash, i, safeBaseName)

		log.Info("uploading to R2", "file", f.Name, "size_mb", f.Size/1_000_000)

		file, err := os.Open(f.Path)
		if err != nil {
			job.Status = jobs.StatusFailed
			job.Error = "disk error"
			p.store.Update(job)
			return
		}

		ct := contentTypeFor(filepath.Ext(f.Name))
		if err := p.r2.StreamUpload(ctx, r2Key, file, ct); err != nil {
			file.Close()
			log.Error("r2 upload failed", "key", r2Key, "err", err)
			job.Status = jobs.StatusFailed
			job.Error = "upload failed"
			p.store.Update(job)
			return
		}
		file.Close()
		uploadedSize += f.Size
		os.Remove(f.Path)

		log.Info("uploaded", "key", r2Key, "size_mb", f.Size/1_000_000)

		streamURLs = append(streamURLs, jobs.Stream{
			FileName:  f.Name,
			Size:      f.Size,
			DirectURL: r2Key,
			SignedURL: p.r2.SignURL(r2Key, 24*time.Hour),
		})
	}

	// Create manifest.
	type manifestFile struct {
		FileName  string `json:"file_name"`
		DirectURL string `json:"direct_url"`
		FileSize  int64  `json:"file_size"`
	}
	var mFiles []manifestFile
	for _, s := range streamURLs {
		mFiles = append(mFiles, manifestFile{FileName: s.FileName, DirectURL: s.DirectURL, FileSize: s.Size})
	}
	manifest := map[string]any{
		"info_hash": job.InfoHash, "name": job.Name,
		"files": mFiles, "created_at": time.Now(),
	}
	manifestJSON, _ := json.MarshalIndent(manifest, "", "  ")
	p.r2.UploadFile(ctx, job.InfoHash+"/manifest.json", strings.NewReader(string(manifestJSON)), "application/json")

	siblings, _ := p.store.ListByInfoHash(job.InfoHash)
	for _, sib := range siblings {
		sib.StreamURLs = streamURLs
		sib.Name = job.Name
		sib.Status = jobs.StatusComplete
		sib.Error = ""
		p.store.Update(sib)
		p.store.SetFileSize(sib.ID, uploadedSize)
		p.enqueueBYOSIfTarget(sib)
	}

	log.Info("job complete", "name", job.Name, "streams", len(streamURLs))
}
