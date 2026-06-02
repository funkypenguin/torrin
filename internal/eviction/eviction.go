package eviction

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/torrin-app/torrin/internal/jobs"
	"github.com/torrin-app/torrin/internal/r2"
)

type Policy struct {
	NeverAccessedTTL int   // Days before deleting never-watched content (default: 7)
	StandardTTL      int   // Days of inactivity for 1-9 access count (default: 30)
	PopularTTL       int   // Days of inactivity for 10+ access count (default: 60)
	StorageCapBytes  int64 // Max total cached bytes before forced eviction (default: 450GB)
}

var DefaultPolicy = Policy{
	NeverAccessedTTL: 3,
	StandardTTL:      14,
	PopularTTL:       45,
	StorageCapBytes:  300 * 1024 * 1024 * 1024,
}

// Engine handles cache eviction.
type Engine struct {
	store  *jobs.Store
	r2     *r2.Client
	policy Policy
}

func New(store *jobs.Store, r2 *r2.Client, policy Policy) *Engine {
	return &Engine{store: store, r2: r2, policy: policy}
}

func (e *Engine) RunDaily(ctx context.Context) {
	slog.Info("eviction: starting daily check")

	candidates, err := e.store.GetEvictionCandidates()
	if err != nil {
		slog.Error("eviction: get candidates", "err", err)
		return
	}

	var evicted, freedBytes int64

	for _, c := range candidates {
		shouldEvict := false
		reason := ""

		// Large files (>50GB) get popular-tier retention regardless of views.
		isLarge := c.FileSize > 50*1024*1024*1024

		if isLarge {
			if c.DaysSinceAccess >= e.policy.PopularTTL {
				shouldEvict = true
				reason = fmt.Sprintf("large file (%dGB), %d days inactive", c.FileSize/(1024*1024*1024), c.DaysSinceAccess)
			}
		} else if c.AccessCount == 0 && c.DaysSinceAccess >= e.policy.NeverAccessedTTL {
			shouldEvict = true
			reason = fmt.Sprintf("never accessed, %d days old", c.DaysSinceAccess)
		} else if c.AccessCount > 0 && c.AccessCount < 10 && c.DaysSinceAccess >= e.policy.StandardTTL {
			shouldEvict = true
			reason = fmt.Sprintf("%d accesses, %d days inactive", c.AccessCount, c.DaysSinceAccess)
		} else if c.AccessCount >= 10 && c.DaysSinceAccess >= e.policy.PopularTTL {
			shouldEvict = true
			reason = fmt.Sprintf("popular (%d accesses) but %d days inactive", c.AccessCount, c.DaysSinceAccess)
		}

		if shouldEvict {
			if err := e.deleteFromR2(ctx, c.InfoHash); err != nil {
				slog.Warn("eviction: delete failed", "hash", c.InfoHash, "err", err)
				continue
			}
			e.store.Delete(c.ID)
			evicted++
			freedBytes += c.FileSize
			slog.Info("evicted", "name", c.Name, "reason", reason, "size_mb", c.FileSize/(1024*1024))
		}
	}

	totalSize, _ := e.store.GetTotalCachedSize()
	if totalSize > e.policy.StorageCapBytes {
		slog.Warn("eviction: over storage cap", "total_gb", totalSize/(1024*1024*1024), "cap_gb", e.policy.StorageCapBytes/(1024*1024*1024))

		candidates, _ = e.store.GetEvictionCandidates()
		for _, c := range candidates {
			if totalSize <= e.policy.StorageCapBytes {
				break
			}
			if err := e.deleteFromR2(ctx, c.InfoHash); err != nil {
				continue
			}
			e.store.Delete(c.ID)
			totalSize -= c.FileSize
			evicted++
			freedBytes += c.FileSize
			slog.Info("budget evicted", "name", c.Name, "size_mb", c.FileSize/(1024*1024))
		}
	}

	slog.Info("eviction: complete", "evicted", evicted, "freed_gb", freedBytes/(1024*1024*1024))
}

func (e *Engine) deleteFromR2(ctx context.Context, infoHash string) error {
	return e.r2.DeletePrefix(ctx, infoHash+"/")
}

func (e *Engine) StartSchedule(ctx context.Context, hour int) {
	go func() {
		for {
			now := time.Now()
			next := time.Date(now.Year(), now.Month(), now.Day(), hour, 0, 0, 0, now.Location())
			if next.Before(now) {
				next = next.Add(24 * time.Hour)
			}
			slog.Info("eviction: next run", "at", next.Format("2006-01-02 15:04"))

			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Until(next)):
				e.RunDaily(ctx)
			}
		}
	}()
}
