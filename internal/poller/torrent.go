package poller

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/torrin-app/torrin/internal/jobs"
	"github.com/torrin-app/torrin/internal/qbit"
)

// pollTorrentJob handles a single torrent job during the poll cycle.
func (p *Poller) pollTorrentJob(ctx context.Context, job *jobs.Job) {
	t, err := p.qb.GetTorrent(job.InfoHash)
	if err != nil {
		if (job.Status == jobs.StatusQueued || job.Status == jobs.StatusPending) && job.Magnet != "" {
			p.tryAddQueued(job)
		} else if job.Status == jobs.StatusProcessing {
			if time.Since(job.UpdatedAt) > 30*time.Second {
				job.Status = jobs.StatusFailed
				job.Error = "torrent removed from download engine"
				p.store.Update(job)
				slog.Info("job marked failed — torrent missing from qbit", "job", job.ID)
			}
		}
		return
	}

	hasRealMetadata := t.Size > 0 && t.Name != ""
	needsSizeCheck := hasRealMetadata && len(job.Files) == 0

	if needsSizeCheck {
		job.Name = t.Name

		if job.MaxBytes > 0 && t.Size > job.MaxBytes {
			maxGB := job.MaxBytes / 1e9
			actualGB := t.Size / 1e9
			slog.Warn("torrent exceeds plan size limit",
				"job", job.ID, "name", t.Name,
				"size_gb", actualGB, "max_gb", maxGB)
			job.Status = jobs.StatusFailed
			job.Error = fmt.Sprintf("torrent size %dGB exceeds your plan limit of %dGB", actualGB, maxGB)
			p.store.Update(job)
			p.deleteAndVerify(job.InfoHash, t)
			return
		}

		files, err := p.qb.GetFiles(job.InfoHash)
		if err == nil && len(files) > 0 {
			job.Files = make([]jobs.File, len(files))
			for i, f := range files {
				job.Files[i] = jobs.File{Index: f.Index, Name: f.Name, Size: f.Size}
			}
		}

		p.qb.Resume(job.InfoHash)
		job.Status = jobs.StatusProcessing
		p.store.Update(job)
		slog.Info("metadata received, size ok, resuming",
			"job", job.ID, "name", t.Name,
			"size_gb", t.Size/1e9,
			"max_gb", job.MaxBytes/1e9)
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
		p.ReleaseFor(job.InfoHash)
		return
	}

	if qbit.IsQueued(t) {
		return
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
			p.ReleaseFor(job.InfoHash)
			return
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
			p.ReleaseFor(job.InfoHash)
			return
		} else if stalledFor > 5*time.Minute {
			p.qb.Reannounce(job.InfoHash)
		}
	}

	if qbit.IsComplete(t) {
		if _, already := p.uploading.LoadOrStore(job.InfoHash, true); already {
			return
		}
		slog.Info("torrent complete, uploading to R2", "job", job.ID, "name", t.Name)
		go func(j *jobs.Job, tor *qbit.Torrent) {
			p.uploadSem <- struct{}{}
			defer func() { <-p.uploadSem }()
			defer p.uploading.Delete(j.InfoHash)
			defer func() {
				if r := recover(); r != nil {
					slog.Error("panic in upload goroutine", "err", r, "job", j.ID)
					j.Status = jobs.StatusFailed
					j.Error = "internal error during upload"
					p.store.Update(j)
					p.ReleaseFor(j.InfoHash)
				}
			}()
			p.uploadAndFinalize(ctx, j, tor)
		}(job, t)
	}
}

func (p *Poller) tryAddQueued(job *jobs.Job) {
	var estimatedSize int64
	for _, f := range job.Files {
		estimatedSize += f.Size
	}
	if estimatedSize == 0 {
		estimatedSize = 5_000_000_000
	}

	if !p.ReserveFor(job.InfoHash, estimatedSize) {
		return
	}

	if err := p.qb.Login(); err != nil {
		p.ReleaseFor(job.InfoHash)
		return
	}
	if err := p.qb.AddMagnet(job.Magnet); err != nil {
		p.ReleaseFor(job.InfoHash)
		slog.Warn("failed to add queued job to qbit", "job", job.ID, "err", err)
		return
	}

	job.Status = jobs.StatusPending
	p.store.Update(job)
	slog.Info("queued job added to qbittorrent", "job", job.ID, "size_gb", estimatedSize/1e9)
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
