package poller

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/torrin-app/torrin/internal/alldebrid"
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
	ad            *alldebrid.Client
	pmKeyProvider func(userID string) string
	tbKeyProvider func(userID string) string
	r2            *r2.Client
	store         *jobs.Store
	interval      time.Duration
	budgetMax     int64
	uploading     sync.Map
	rdSkip        sync.Map
	adSkip        sync.Map
	pmSkip        sync.Map
	tbSkip        sync.Map
	rdClients     sync.Map
	uploadSem     chan struct{}
	UploadWg      sync.WaitGroup
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

func (p *Poller) SetAllDebrid(client *alldebrid.Client) {
	p.ad = client
}

func (p *Poller) SetPremiumizeKeyProvider(fn func(userID string) string) {
	p.pmKeyProvider = fn
}

func (p *Poller) SetTorBoxKeyProvider(fn func(userID string) string) {
	p.tbKeyProvider = fn
}

func New(qb *qbit.Client, r2 *r2.Client, store *jobs.Store, interval time.Duration) *Poller {
	return &Poller{
		qb: qb, r2: r2, store: store, interval: interval,
		budgetMax: 1_000_000_000_000,
		uploadSem: make(chan struct{}, 5),
	}
}

func (p *Poller) BudgetUsed() int64 {
	var total int64
	for _, status := range []jobs.Status{jobs.StatusPending, jobs.StatusProcessing, jobs.StatusQueued} {
		active, _ := p.store.ListByStatus(status)
		for _, j := range active {
			size := j.FileSize
			if size == 0 {
				for _, f := range j.Files {
					size += f.Size
				}
			}
			if size == 0 {
				size = 5_000_000_000
			}
			total += size
		}
	}
	return total
}

func (p *Poller) BudgetAvailable() int64 {
	avail := p.budgetMax - p.BudgetUsed()
	if avail < 0 {
		return 0
	}
	return avail
}

func (p *Poller) failDownload(ctx context.Context, job *jobs.Job, reason string) {
	if ctx.Err() != nil {
		job.Status = jobs.StatusPending
		job.Error = ""
		p.store.Update(job)
		return
	}
	job.Status = jobs.StatusFailed
	job.Error = reason
	p.store.Update(job)
}

func (p *Poller) GetRDClient(apiKey string) *realdebrid.Client {
	if v, ok := p.rdClients.Load(apiKey); ok {
		return v.(*realdebrid.Client)
	}
	client := realdebrid.NewClient(apiKey)
	actual, _ := p.rdClients.LoadOrStore(apiKey, client)
	return actual.(*realdebrid.Client)
}

func (p *Poller) Run(ctx context.Context) {
	slog.Info("poller started", "interval", p.interval, "budget_gb", p.budgetMax/1e9)

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
		// For hybrid v2 torrents, qBit uses a different hash than our DB.
		// Check if any active job's magnet contains this hash as a btmh v2 hash.
		if (err != nil || job == nil) && t.Hash != "" {
			active, _ := p.store.ListByStatus(jobs.StatusProcessing)
			pending, _ := p.store.ListByStatus(jobs.StatusPending)
			for _, j := range append(active, pending...) {
				if v2 := extractV2Hash(j.Magnet); v2 == t.Hash {
					job = j
					err = nil
					break
				}
			}
		}
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

		if job.Source == "hoster" {
			p.pollHosterJob(ctx, job)
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

		if job.Status == jobs.StatusPending && p.ad != nil {
			if _, skip := p.adSkip.Load(job.InfoHash); !skip {
				if p.tryAllDebrid(ctx, job) {
					continue
				}
				p.adSkip.Store(job.InfoHash, true)
			}
		}

		if job.Status == jobs.StatusPending && p.pmKeyProvider != nil {
			if _, skip := p.pmSkip.Load(job.InfoHash); !skip {
				if p.tryPremiumize(ctx, job) {
					continue
				}
				p.pmSkip.Store(job.InfoHash, true)
			}
		}

		if job.Status == jobs.StatusPending && p.tbKeyProvider != nil {
			if _, skip := p.tbSkip.Load(job.InfoHash); !skip {
				if p.tryTorBox(ctx, job) {
					continue
				}
				p.tbSkip.Store(job.InfoHash, true)
			}
		}

		if !qbOk {
			continue
		}
		p.pollTorrentJob(ctx, job)
	}
}
