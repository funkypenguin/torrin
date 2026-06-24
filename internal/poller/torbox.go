package poller

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/torrin-app/torrin/internal/jobs"
	"github.com/torrin-app/torrin/internal/torbox"
)

func (p *Poller) tryTorBox(ctx context.Context, job *jobs.Job) bool {
	if p.tbKeyProvider == nil || job.Magnet == "" {
		return false
	}

	apiKey := p.tbKeyProvider(job.UserID)
	if apiKey == "" {
		return false
	}

	if _, checking := p.uploading.Load("tb-check-" + job.InfoHash); checking {
		return false
	}
	p.uploading.Store("tb-check-"+job.InfoHash, true)
	defer p.uploading.Delete("tb-check-" + job.InfoHash)

	log := slog.With("job", job.ID, "hash", job.InfoHash, "source", "torbox")

	client := torbox.NewClient(apiKey)

	// Check cache.
	cached, err := client.CheckCached(ctx, []string{job.InfoHash})
	if err != nil {
		log.Debug("tb cache check failed", "err", err)
		return false
	}
	if len(cached) == 0 {
		log.Debug("not cached on torbox")
		return false
	}

	log.Info("content cached on torbox", "name", cached[0].Name, "size_mb", cached[0].Size/1_000_000)

	// Create torrent to get download links.
	created, err := client.CreateTorrent(ctx, job.Magnet)
	if err != nil {
		log.Warn("tb create torrent failed", "err", err)
		return false
	}

	torrentID := created.Data.TorrentID

	// Size check.
	if job.MaxBytes > 0 && cached[0].Size > job.MaxBytes {
		log.Warn("tb torrent exceeds plan limit", "size_gb", cached[0].Size/1e9, "max_gb", job.MaxBytes/1e9)
		job.Status = jobs.StatusFailed
		job.Error = fmt.Sprintf("torrent size %dGB exceeds your plan limit of %dGB", cached[0].Size/1e9, job.MaxBytes/1e9)
		p.store.Update(job)
		client.DeleteTorrent(ctx, torrentID)
		return true
	}

	if _, already := p.uploading.LoadOrStore(job.InfoHash, true); already {
		client.DeleteTorrent(ctx, torrentID)
		return true
	}

	job.Status = jobs.StatusProcessing
	job.Error = "downloading"
	if job.Name == "" {
		if created.Data.Name != "" {
			job.Name = created.Data.Name
		} else if cached[0].Name != "" {
			job.Name = cached[0].Name
		}
	}
	job.FileSize = cached[0].Size
	p.store.SetFileSize(job.ID, cached[0].Size)
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
		defer client.DeleteTorrent(ctx, torrentID)
		defer func() {
			if r := recover(); r != nil {
				slog.Error("panic in TB download", "err", r, "job", job.ID)
				job.Status = jobs.StatusFailed
				job.Error = "internal error"
				p.store.Update(job)
			}
		}()

		dlURL, err := client.RequestDownloadLinkWithRetry(ctx, torrentID, 0)
		if err != nil {
			log.Error("tb request dl failed (all CDNs)", "err", err)
			p.failDownload(ctx, job, "download link failed")
			return
		}

		tmpDir := filepath.Join(p.rdDownloadDir, job.InfoHash)
		os.MkdirAll(tmpDir, 0755)
		defer func() {
			if ctx.Err() == nil {
				os.RemoveAll(tmpDir)
			}
		}()

		name := cached[0].Name
		if name == "" {
			name = job.InfoHash + ".mkv"
		}
		localPath := filepath.Join(tmpDir, filepath.Base(name))

		log.Info("downloading from tb", "file", name, "size_mb", cached[0].Size/1_000_000)

		if err := p.downloadFromURL(ctx, client, dlURL, localPath, job, cached[0].Size, 0, 1); err != nil {
			log.Error("tb download failed", "err", err)
			p.failDownload(ctx, job, "download failed")
			return
		}

		info, _ := os.Stat(localPath)
		var size int64
		if info != nil {
			size = info.Size()
		}

		files := []downloadedFile{
			{Name: filepath.Base(name), Path: localPath, Size: size},
		}
		p.uploadAndFinalizeFiles(ctx, job, files, log)
	}()

	return true
}
