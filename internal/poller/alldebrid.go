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
)

// tryAllDebrid checks if content is instantly available on AllDebrid.
// Used as a fallback when RD fails (infringing_file, etc.).
func (p *Poller) tryAllDebrid(ctx context.Context, job *jobs.Job) bool {
	if p.ad == nil || job.Magnet == "" {
		return false
	}

	if _, checking := p.uploading.Load("ad-check-" + job.InfoHash); checking {
		return false
	}
	p.uploading.Store("ad-check-"+job.InfoHash, true)
	defer p.uploading.Delete("ad-check-" + job.InfoHash)

	log := slog.With("job", job.ID, "hash", job.InfoHash, "source", "alldebrid")

	// Step 1: Add magnet to AD.
	added, err := p.ad.AddMagnet(ctx, job.Magnet)
	if err != nil {
		log.Warn("ad add magnet failed", "err", err)
		return false
	}

	if !added.Ready {
		log.Info("not cached on alldebrid, falling through to qbit")
		p.ad.DeleteMagnet(ctx, added.ID)
		return false
	}

	log.Info("content cached on alldebrid", "name", added.Name, "size_mb", added.Size/1_000_000)

	// Step 2: Get files.
	files, err := p.ad.GetMagnetFiles(ctx, added.ID)
	if err != nil {
		log.Warn("ad get files failed", "err", err)
		p.ad.DeleteMagnet(ctx, added.ID)
		return false
	}

	// Filter video files only.
	type videoFile struct {
		Name string
		Link string
		Size int64
	}
	var videoFiles []videoFile
	var totalVideoSize int64
	for _, f := range files {
		if isVideoFile(f.Name) {
			videoFiles = append(videoFiles, videoFile{Name: f.Name, Link: f.Link, Size: f.Size})
			totalVideoSize += f.Size
		}
	}

	if len(videoFiles) == 0 {
		log.Info("ad: no video files found, falling through to qbit")
		p.ad.DeleteMagnet(ctx, added.ID)
		return false
	}

	// Size check.
	if job.MaxBytes > 0 && totalVideoSize > job.MaxBytes {
		log.Warn("ad torrent exceeds plan limit", "size_gb", totalVideoSize/1e9, "max_gb", job.MaxBytes/1e9)
		job.Status = jobs.StatusFailed
		job.Error = fmt.Sprintf("torrent size %dGB exceeds your plan limit of %dGB", totalVideoSize/1e9, job.MaxBytes/1e9)
		p.store.Update(job)
		p.ad.DeleteMagnet(ctx, added.ID)
		return true
	}

	// Start download in background.
	if _, already := p.uploading.LoadOrStore(job.InfoHash, true); already {
		p.ad.DeleteMagnet(ctx, added.ID)
		return true
	}

	job.Status = jobs.StatusProcessing
	job.Error = "downloading"
	if added.Name != "" && job.Name == "" {
		job.Name = added.Name
	}
	job.FileSize = totalVideoSize
	p.store.SetFileSize(job.ID, totalVideoSize)
	p.store.Update(job)

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
		defer p.ad.DeleteMagnet(ctx, added.ID)
		defer func() {
			if r := recover(); r != nil {
				slog.Error("panic in AD download", "err", r, "job", job.ID)
				job.Status = jobs.StatusFailed
				job.Error = "internal error"
				p.store.Update(job)
			}
		}()

		tmpDir := filepath.Join(p.rdDownloadDir, job.InfoHash)
		os.MkdirAll(tmpDir, 0755)
		defer func() {
			if ctx.Err() == nil {
				os.RemoveAll(tmpDir)
			}
		}()

		type dlFile struct {
			Name string
			Path string
			Size int64
		}
		var downloadedFiles []dlFile

		for i, vf := range videoFiles {
			if ctx.Err() != nil {
				return
			}

			// Unlock the download link.
			unlocked, err := p.ad.Unlock(ctx, vf.Link)
			if err != nil {
				log.Warn("ad unlock failed", "file", vf.Name, "err", err)
				continue
			}

			log.Info("downloading from ad", "file", unlocked.Filename, "size_mb", unlocked.FileSize/1_000_000)

			localPath := filepath.Join(tmpDir, filepath.Base(unlocked.Filename))
			if err := p.downloadHosterFileIdx(ctx, unlocked.Link, localPath, job, unlocked.FileSize, i, len(videoFiles)); err != nil {
				log.Error("ad download failed", "file", unlocked.Filename, "err", err)
				p.failDownload(ctx, job, "download failed")
				return
			}

			info, _ := os.Stat(localPath)
			var size int64
			if info != nil {
				size = info.Size()
			}

			downloadedFiles = append(downloadedFiles, dlFile{Name: unlocked.Filename, Path: localPath, Size: size})
		}

		if len(downloadedFiles) == 0 {
			log.Error("no video files downloaded from ad")
			job.Status = jobs.StatusFailed
			job.Error = "no video files downloaded"
			p.store.Update(job)
			return
		}

		// Upload to R2.
		job.Error = "uploading to cache"
		p.store.Update(job)

		var streamURLs []jobs.Stream
		var uploadedSize int64

		for i, f := range downloadedFiles {
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

		// Create manifest and finalize.
		type manifestFile struct {
			FileName  string `json:"file_name"`
			DirectURL string `json:"direct_url"`
			FileSize  int64  `json:"file_size"`
		}
		var mFiles []manifestFile
		for i, s := range streamURLs {
			var sz int64
			if i < len(downloadedFiles) {
				sz = downloadedFiles[i].Size
			}
			mFiles = append(mFiles, manifestFile{FileName: s.FileName, DirectURL: s.DirectURL, FileSize: sz})
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

		log.Info("job complete via alldebrid", "name", job.Name, "streams", len(streamURLs))
	}()

	return true
}

func (p *Poller) downloadHosterFileIdx(ctx context.Context, downloadURL, localPath string, job *jobs.Job, totalSize int64, fileIdx, fileCount int) error {
	open := func(offset int64) (io.ReadCloser, int64, bool, error) {
		return p.ad.DownloadFileRange(ctx, downloadURL, offset)
	}
	return p.downloadToFile(ctx, open, localPath, job, totalSize, fileIdx, fileCount)
}
