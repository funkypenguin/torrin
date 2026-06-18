package poller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/torrin-app/torrin/internal/jobs"
	"github.com/torrin-app/torrin/internal/usenet"
)

func (p *Poller) pollUsenetJob(ctx context.Context, job *jobs.Job) {
	if p.usenet == nil {
		return
	}

	dl := p.usenet.GetDownload(job.InfoHash)

	// Pending usenet job: start the download.
	if dl == nil && job.Status == jobs.StatusPending {
		if job.NZBData == nil {
			job.Status = jobs.StatusFailed
			job.Error = "no NZB data"
			p.store.Update(job)
			return
		}

		if p.BudgetAvailable() < job.FileSize {
			job.Status = jobs.StatusQueued
			p.store.Update(job)
			return
		}

		_, err := p.usenet.Submit(ctx, job.UserID, job.NZBData, job.Name)
		if err != nil {

			job.Status = jobs.StatusFailed
			job.Error = fmt.Sprintf("usenet: %v", err)
			p.store.Update(job)
			return
		}

		job.Status = jobs.StatusProcessing
		p.store.Update(job)
		return
	}

	// Queued: try again when budget available.
	if dl == nil && job.Status == jobs.StatusQueued {
		if p.BudgetAvailable() > 1_000_000_000 {
			job.Status = jobs.StatusPending
			p.store.Update(job)
		}
		return
	}

	if dl == nil {
		// Download not tracked (lost on restart). Re-submit once if we have NZB data.
		// Don't re-submit if the job already has an error (was already attempted).
		if job.NZBData != nil && job.Error == "" && time.Since(job.UpdatedAt) > 30*time.Second {
			slog.Info("re-submitting usenet job after restart", "job", job.ID)
			job.Status = jobs.StatusPending
			p.store.Update(job)
		} else if time.Since(job.UpdatedAt) > 2*time.Minute {
			job.Status = jobs.StatusFailed
			if job.Error == "" {
				job.Error = "usenet download lost"
			}
			p.store.Update(job)
		}
		return
	}

	// Read the download's mutable state once, under its lock; the manager's
	// download goroutine writes these fields concurrently.
	snap := dl.Snapshot()

	switch snap.Status {
	case usenet.StatusDownloading:
		job.Status = jobs.StatusProcessing
		// Show progress and speed.
		pct := int(snap.Progress * 100)
		progressMsg := fmt.Sprintf("downloading — %d%% (%d B/s)", pct, snap.Speed)
		if job.Error != progressMsg {
			job.Error = progressMsg
			p.store.Update(job)
		}

	case usenet.StatusPostProcessing:
		if job.Error != "post-processing" {
			job.Error = "post-processing"
			p.store.Update(job)
		}

	case usenet.StatusFailed:
		if ctx.Err() != nil {
			job.Status = jobs.StatusPending
			job.Error = ""
			p.store.Update(job)
			p.usenet.CleanupFiles(job.InfoHash)
			return
		}
		job.Status = jobs.StatusFailed
		job.Error = snap.Error
		p.store.Update(job)
		p.usenet.CleanupFiles(job.InfoHash)

	case usenet.StatusComplete:
		if _, already := p.uploading.LoadOrStore(job.InfoHash, true); already {
			return
		}
		slog.Info("usenet download complete, uploading to R2", "job", job.ID, "name", job.Name)
		p.UploadWg.Add(1)
		go func(j *jobs.Job, files []usenet.OutputFile) {
			defer p.UploadWg.Done()
			select {
			case p.uploadSem <- struct{}{}:
			case <-ctx.Done():
				p.uploading.Delete(j.InfoHash)
				return
			}
			defer func() { <-p.uploadSem }()
			defer p.uploading.Delete(j.InfoHash)
			defer func() {
				if r := recover(); r != nil {
					slog.Error("panic in usenet upload goroutine", "err", r, "job", j.ID)
					j.Status = jobs.StatusFailed
					j.Error = "internal error during upload"
					p.store.Update(j)
				}
			}()
			p.uploadLocalFiles(ctx, j, files)
			p.usenet.CleanupFiles(j.InfoHash)
		}(job, snap.Files)
	}
}
