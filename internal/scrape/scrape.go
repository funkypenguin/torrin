package scrape

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"
	"time"

	"github.com/etix/goscrape"
)

type Cache struct {
	db *sql.DB
}

func NewCache(db *sql.DB) *Cache {
	db.Exec(`CREATE TABLE IF NOT EXISTS scrape_cache (
		info_hash TEXT PRIMARY KEY,
		seeders   INTEGER NOT NULL DEFAULT 0,
		leechers  INTEGER NOT NULL DEFAULT 0,
		completed INTEGER NOT NULL DEFAULT 0,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	)`)
	return &Cache{db: db}
}

func (c *Cache) Get(hashes []string) (cached map[string]Result, missing []string) {
	cached = make(map[string]Result)
	cutoff := time.Now().Add(-24 * time.Hour)

	for _, h := range hashes {
		var r Result
		var updatedAt time.Time
		err := c.db.QueryRow(
			`SELECT seeders, leechers, completed, updated_at FROM scrape_cache WHERE info_hash=?`, h,
		).Scan(&r.Seeders, &r.Leechers, &r.Completed, &updatedAt)
		if err == nil && updatedAt.After(cutoff) {
			cached[h] = r
		} else {
			missing = append(missing, h)
		}
	}
	return
}

func (c *Cache) Set(results map[string]Result) {
	for h, r := range results {
		c.db.Exec(`INSERT INTO scrape_cache (info_hash, seeders, leechers, completed, updated_at)
			VALUES (?, ?, ?, ?, ?)
			ON CONFLICT(info_hash) DO UPDATE SET seeders=?, leechers=?, completed=?, updated_at=?`,
			h, r.Seeders, r.Leechers, r.Completed, time.Now(),
			r.Seeders, r.Leechers, r.Completed, time.Now(),
		)
	}
}

var defaultTrackers = []string{
	"udp://tracker.opentrackr.org:1337/announce",
	"udp://open.stealth.si:80/announce",
	"udp://tracker.torrent.eu.org:451/announce",
	"udp://tracker.opentorrent.top:6969/announce",
	"udp://open.demonii.com:1337/announce",
	"udp://explodie.org:6969/announce",
	"udp://tracker.dler.com:6969/announce",
	"udp://tracker-udp.gbitt.info:80/announce",
}

type Result struct {
	Seeders   int `json:"seeders"`
	Leechers  int `json:"leechers"`
	Completed int `json:"completed"`
}

// BatchScrape scrapes multiple trackers for seeder/leecher counts.
// Returns the best (highest seeder) result per hash.
// Max 74 hashes per call (UDP protocol limit).
func BatchScrape(ctx context.Context, hashes []string) map[string]Result {
	if len(hashes) == 0 {
		return nil
	}
	if len(hashes) > 74 {
		hashes = hashes[:74]
	}

	var infohashes [][]byte
	for _, h := range hashes {
		infohashes = append(infohashes, []byte(h))
	}

	results := make(map[string]Result)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, tracker := range defaultTrackers {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			s, err := goscrape.New(addr)
			if err != nil {
				slog.Debug("scrape: tracker connect failed", "tracker", addr, "err", err)
				return
			}

			done := make(chan struct{})
			var res []*goscrape.ScrapeResult
			var scrapeErr error
			go func() {
				res, scrapeErr = s.Scrape(infohashes...)
				close(done)
			}()

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				slog.Debug("scrape: tracker timeout", "tracker", addr)
				return
			case <-ctx.Done():
				return
			}

			if scrapeErr != nil {
				slog.Debug("scrape: tracker failed", "tracker", addr, "err", scrapeErr)
				return
			}

			mu.Lock()
			for _, r := range res {
				hash := string(r.Infohash)
				existing, ok := results[hash]
				if !ok || int(r.Seeders) > existing.Seeders {
					results[hash] = Result{
						Seeders:   int(r.Seeders),
						Leechers:  int(r.Leechers),
						Completed: int(r.Completed),
					}
				}
			}
			mu.Unlock()
		}(tracker)
	}

	wg.Wait()
	return results
}
