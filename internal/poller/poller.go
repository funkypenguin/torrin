package poller

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/torrin-app/torrin/internal/jobs"
	"github.com/torrin-app/torrin/internal/qbit"
	"github.com/torrin-app/torrin/internal/r2"
	"github.com/torrin-app/torrin/internal/realdebrid"
	"github.com/torrin-app/torrin/internal/usenet"
)

type Poller struct {
	qb            *qbit.Client
	usenet        *usenet.Manager
	rd            *realdebrid.Client
	rdKeyProvider realdebrid.KeyProvider
	rdHashLookup  realdebrid.HashLookup
	rdDownloadDir string
	r2            *r2.Client
	store         *jobs.Store
	interval      time.Duration
	budgetMax     int64
	budgetUsed    int64
	uploading     sync.Map
	rdSkip        sync.Map
}

func (p *Poller) SetUsenetManager(m *usenet.Manager) {
	p.usenet = m
}

func (p *Poller) SetRealDebrid(client *realdebrid.Client, downloadDir string) {
	p.rd = client
	p.rdDownloadDir = downloadDir
}

func (p *Poller) SetRDKeyProvider(provider realdebrid.KeyProvider) {
	p.rdKeyProvider = provider
}

func (p *Poller) SetRDHashLookup(lookup realdebrid.HashLookup) {
	p.rdHashLookup = lookup
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
	qbOk := p.qb.Login() == nil

	activeJobs, _ := p.store.ListByStatus(jobs.StatusProcessing)
	pendingJobs, _ := p.store.ListByStatus(jobs.StatusPending)
	queuedJobs, _ := p.store.ListByStatus(jobs.StatusQueued)

	allActive := append(append(activeJobs, pendingJobs...), queuedJobs...)

	for _, job := range allActive {
		if job.InfoHash == "" {
			continue
		}

		if job.Source == "usenet" {
			p.pollUsenetJob(ctx, job)
			continue
		}

		if _, rdActive := p.uploading.Load(job.InfoHash); rdActive {
			continue
		}

		if job.Status == jobs.StatusPending && p.rd != nil {
			if _, skip := p.rdSkip.Load(job.InfoHash); !skip {
				if p.tryRealDebrid(ctx, job) {
					continue
				}
				p.rdSkip.Store(job.InfoHash, true)
			}
		}

		if !qbOk {
			continue
		}
		p.pollTorrentJob(ctx, job)
	}
}
