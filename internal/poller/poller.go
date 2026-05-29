package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/torrin-app/torrin/internal/jobs"
	"github.com/torrin-app/torrin/internal/qbit"
	"github.com/torrin-app/torrin/internal/r2"
)

type Poller struct {
	qb         *qbit.Client
	r2         *r2.Client
	store      *jobs.Store
	interval   time.Duration
	budgetMax  int64
	budgetUsed int64
	uploading  sync.Map
}

func New(qb *qbit.Client, r2 *r2.Client, store *jobs.Store, interval time.Duration) *Poller {
	return &Poller{
		qb: qb, r2: r2, store: store, interval: interval,
		budgetMax: 1024 * 1024 * 1024 * 1024,
	}
}

func (p *Poller) BudgetAvailable() int64 {
	used := atomic.LoadInt64(&p.budgetUsed)
	avail := p.budgetMax - used
	if avail < 0 {
		return 0
	}
	return avail
}

func (p *Poller) BudgetUsed() int64 {
	return atomic.LoadInt64(&p.budgetUsed)
}

func (p *Poller) Reserve(bytes int64) bool {
	for {
		used := atomic.LoadInt64(&p.budgetUsed)
		if used+bytes > p.budgetMax {
			return false
		}
		if atomic.CompareAndSwapInt64(&p.budgetUsed, used, used+bytes) {
			return true
		}
	}
}

func (p *Poller) Release(bytes int64) {
	atomic.AddInt64(&p.budgetUsed, -bytes)
}

func (p *Poller) Run(ctx context.Context) {
	slog.Info("poller started", "interval", p.interval, "budget_gb", p.budgetMax/(1024*1024*1024))

	p.cleanupOrphans()

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("poller stopped")
			return
		case <-ticker.C:
			p.poll(ctx)
		}
	}
}

// cleanupOrphans removes stuck/orphaned torrents from qBit on startup.
// Only removes torrents that are safe to delete:
//   - no matching job in DB (true orphan)
//   - job already failed
//   - stuck on metadata with no progress
func (p *Poller) cleanupOrphans() {
	if err := p.qb.Login(); err != nil {
		return
	}

	torrents, err := p.qb.ListTorrents()
	if err != nil {
		return
	}

	cleaned := 0
	for _, t := range torrents {
		job, err := p.store.GetByInfoHash(t.Hash)
		if err != nil || job == nil {
			slog.Info("cleanup orphan", "hash", t.Hash, "name", t.Name)
			p.qb.Delete(t.Hash)
			cleaned++
			continue
		}
		if job.Status == jobs.StatusFailed {
			slog.Info("cleanup failed job torrent", "hash", t.Hash, "name", t.Name)
			p.qb.Delete(t.Hash)
			cleaned++
			continue
		}
		if qbit.IsFetchingMetadata(&t) && t.Size == 0 {
			slog.Info("cleanup stuck metadata", "hash", t.Hash, "name", t.Name)
			job.Status = jobs.StatusFailed
			job.Error = "could not find torrent metadata"
			p.store.Update(job)
			p.qb.Delete(t.Hash)
			cleaned++
		}
	}

	if cleaned > 0 {
		slog.Info("startup cleanup done", "removed", cleaned)
	}
}

func (p *Poller) poll(ctx context.Context) {
	if err := p.qb.Login(); err != nil {
		slog.Warn("qbit login failed", "err", err)
		return
	}

	activeJobs, _ := p.store.ListByStatus(jobs.StatusProcessing)
	pendingJobs, _ := p.store.ListByStatus(jobs.StatusPending)
	queuedJobs, _ := p.store.ListByStatus(jobs.StatusQueued)

	allActive := append(append(activeJobs, pendingJobs...), queuedJobs...)

	for _, job := range allActive {
		if job.InfoHash == "" {
			continue
		}

		t, err := p.qb.GetTorrent(job.InfoHash)
		if err != nil {
			if job.Status == jobs.StatusQueued && job.Magnet != "" {
				p.tryAddQueued(job)
			} else if job.Status == jobs.StatusProcessing || job.Status == jobs.StatusPending {
				if time.Since(job.UpdatedAt) > 30*time.Second {
					job.Status = jobs.StatusFailed
					job.Error = "torrent removed from download engine"
					p.store.Update(job)
					slog.Info("job marked failed — torrent missing from qbit", "job", job.ID)
				}
			}
			continue
		}

		hasRealMetadata := t.Size > 0 && t.Name != ""
		needsSizeCheck := hasRealMetadata && len(job.Files) == 0

		if needsSizeCheck {
			job.Name = t.Name

			if job.MaxBytes > 0 && t.Size > job.MaxBytes {
				maxGB := job.MaxBytes / (1024 * 1024 * 1024)
				actualGB := t.Size / (1024 * 1024 * 1024)
				slog.Warn("torrent exceeds plan size limit",
					"job", job.ID, "name", t.Name,
					"size_gb", actualGB, "max_gb", maxGB)
				job.Status = jobs.StatusFailed
				job.Error = fmt.Sprintf("torrent size %dGB exceeds your plan limit of %dGB", actualGB, maxGB)
				p.store.Update(job)
				p.deleteAndVerify(job.InfoHash, t)
				continue
			}

			files, err := p.qb.GetFiles(job.InfoHash)
			if err == nil && len(files) > 0 {
				job.Files = make([]jobs.File, len(files))
				for i, f := range files {
					job.Files[i] = jobs.File{Index: f.Index, Name: f.Name, Size: f.Size}
				}
			}

			p.qb.Resume(job.InfoHash)
			p.store.Update(job)
			slog.Info("metadata received, size ok, resuming",
				"job", job.ID, "name", t.Name,
				"size_gb", t.Size/(1024*1024*1024),
				"max_gb", job.MaxBytes/(1024*1024*1024))
		} else if t.Name != "" && job.Name == "" {
			job.Name = t.Name
			p.store.Update(job)
		}

		if qbit.IsDownloading(t) && job.Status != jobs.StatusProcessing {
			job.Status = jobs.StatusProcessing
			job.Error = ""
			p.store.Update(job)
		}

		if t.DlSpeed > 0 && job.Error == "stalled — waiting for peers" {
			job.Error = ""
			p.store.Update(job)
		}

		if qbit.IsError(t) {
			job.Status = jobs.StatusFailed
			job.Error = fmt.Sprintf("torrent error: %s", t.State)
			p.store.Update(job)
			p.deleteAndVerify(job.InfoHash, t)
			p.Release(t.Size)
			continue
		}

		if qbit.IsQueued(t) {
			continue
		}

		// Stall handling — progressive recovery.
		stalledFor := time.Since(job.UpdatedAt)
		isStuck := qbit.IsStalled(t) || (t.DlSpeed == 0 && t.Progress > 0.95 && t.Progress < 1.0)
		if isStuck {
			if stalledFor > 4*time.Hour {
				// 4h stalled — give up.
				slog.Warn("torrent stalled, removing", "job", job.ID, "name", t.Name, "stalled", stalledFor.Round(time.Minute))
				job.Status = jobs.StatusFailed
				job.Error = "torrent stalled — no peers available"
				p.store.Update(job)
				p.deleteAndVerify(job.InfoHash, t)
				p.Release(t.Size)
				continue
			} else if stalledFor > 2*time.Hour && job.Error != "restarting stalled torrent" {
				// 2h stalled — force stop+start (once).
				slog.Info("force restarting stalled torrent", "job", job.ID, "name", t.Name)
				p.qb.Pause(job.InfoHash)
				time.Sleep(2 * time.Second)
				p.qb.Resume(job.InfoHash)
				job.Error = "restarting stalled torrent"
				p.store.Update(job)
			} else if stalledFor > 1*time.Hour {
				// 1h stalled — mark as stalled, keep reannouncing.
				if job.Error != "stalled — waiting for peers" && job.Error != "restarting stalled torrent" {
					slog.Warn("torrent stalled for 1h", "job", job.ID, "name", t.Name)
					job.Error = "stalled — waiting for peers"
					p.store.Update(job)
				}
				p.qb.Reannounce(job.InfoHash)
			} else if stalledFor > 15*time.Minute {
				// 15m — second reannounce.
				p.qb.Reannounce(job.InfoHash)
			} else if stalledFor > 5*time.Minute {
				// 5m — first reannounce attempt.
				p.qb.Reannounce(job.InfoHash)
			}
		}

		if qbit.IsFetchingMetadata(t) {
			if stalledFor > 15*time.Minute {
				slog.Warn("metadata timeout after 15m", "job", job.ID)
				job.Status = jobs.StatusFailed
				job.Error = "could not find torrent metadata — invalid or dead magnet"
				p.store.Update(job)
				p.deleteAndVerify(job.InfoHash, t)
				p.Release(t.Size)
				continue
			} else if stalledFor > 5*time.Minute {
				p.qb.Reannounce(job.InfoHash)
			}
		}

		if qbit.IsComplete(t) {
			if _, already := p.uploading.LoadOrStore(job.InfoHash, true); already {
				continue
			}
			slog.Info("torrent complete, uploading to R2", "job", job.ID, "name", t.Name)
			go func(j *jobs.Job, tor *qbit.Torrent) {
				defer p.uploading.Delete(j.InfoHash)
				p.uploadAndFinalize(ctx, j, tor)
			}(job, t)
		}
	}
}

func (p *Poller) tryAddQueued(job *jobs.Job) {
	var estimatedSize int64
	for _, f := range job.Files {
		estimatedSize += f.Size
	}
	if estimatedSize == 0 {
		estimatedSize = 5 * 1024 * 1024 * 1024
	}

	if !p.Reserve(estimatedSize) {
		return
	}

	if err := p.qb.Login(); err != nil {
		p.Release(estimatedSize)
		return
	}
	if err := p.qb.AddMagnet(job.Magnet); err != nil {
		p.Release(estimatedSize)
		slog.Warn("failed to add queued job to qbit", "job", job.ID, "err", err)
		return
	}

	job.Status = jobs.StatusPending
	p.store.Update(job)
	slog.Info("queued job added to qbittorrent", "job", job.ID, "size_gb", estimatedSize/(1024*1024*1024))
}

func (p *Poller) uploadAndFinalize(ctx context.Context, job *jobs.Job, t *qbit.Torrent) {
	files, err := p.qb.GetFiles(job.InfoHash)
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

		slog.Info("uploaded", "key", r2Key, "size_mb", f.Size/(1024*1024))

		streamURLs = append(streamURLs, jobs.Stream{
			FileName:  baseName,
			DirectURL: r2Key,
			SignedURL: p.r2.SignURL(r2Key, 24*time.Hour),
		})
	}

	if len(streamURLs) == 0 {
		job.Status = jobs.StatusFailed
		job.Error = "no video files found"
		p.store.Update(job)
		p.deleteAndVerify(job.InfoHash, t)
		p.Release(t.Size)
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

	p.deleteAndVerify(job.InfoHash, t)
	p.Release(t.Size)

	slog.Info("job complete", "job", job.ID, "name", job.Name, "streams", len(streamURLs), "users", len(siblings))
}

func isVideoFile(name string) bool {
	ext := strings.ToLower(filepath.Ext(name))
	switch ext {
	case ".mkv", ".mp4", ".avi", ".mov", ".wmv", ".flv", ".webm", ".m4v", ".ts":
		return true
	}
	return false
}

func contentTypeFor(ext string) string {
	switch strings.ToLower(ext) {
	case ".mkv":
		return "video/x-matroska"
	case ".mp4", ".m4v":
		return "video/mp4"
	case ".avi":
		return "video/x-msvideo"
	case ".webm":
		return "video/webm"
	default:
		return "application/octet-stream"
	}
}

func (p *Poller) deleteAndVerify(hash string, t *qbit.Torrent) {
	if err := p.qb.Delete(hash); err != nil {
		slog.Error("qbit delete failed", "hash", hash, "err", err)
	}

	time.Sleep(2 * time.Second)

	if _, err := p.qb.GetTorrent(hash); err == nil {
		slog.Warn("torrent still in qbit after delete, retrying", "hash", hash)
		p.qb.Delete(hash)
		time.Sleep(1 * time.Second)
	}

	contentPath := t.ContentPath
	if contentPath == "" {
		contentPath = filepath.Join(t.SavePath, t.Name)
	}

	if _, err := os.Stat(contentPath); err == nil {
		slog.Warn("files still on disk after qbit delete, removing manually", "path", contentPath)
		if err := os.RemoveAll(contentPath); err != nil {
			slog.Error("manual cleanup failed", "path", contentPath, "err", err)
		} else {
			slog.Info("manual cleanup succeeded", "path", contentPath)
		}
	}
}
