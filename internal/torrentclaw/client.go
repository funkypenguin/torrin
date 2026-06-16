// Package torrentclaw is a thin client for TorrentClaw's debrid cache-check API.
// We use it to detect whether a hash is cached on a debrid provider.
//
// Results are cached in-memory for a short TTL so the same hash searched
// repeatedly doesn't re-hit TorrentClaw's rate/daily limits.
package torrentclaw

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const checkCacheURL = "https://torrentclaw.com/api/v1/debrid/check-cache"

const cacheTTL = 30 * time.Minute

const userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"

type cacheEntry struct {
	cached bool
	exp    time.Time
}

type Client struct {
	apiKey     string
	httpClient *http.Client

	mu    sync.Mutex
	cache map[string]cacheEntry
}

func NewClient(apiKey, proxyURL string) *Client {
	transport := &http.Transport{}
	if proxyURL != "" {
		if p, err := url.Parse(proxyURL); err == nil {
			transport.Proxy = http.ProxyURL(p)
		}
	}
	c := &Client{
		apiKey:     apiKey,
		httpClient: &http.Client{Timeout: 12 * time.Second, Transport: transport},
		cache:      make(map[string]cacheEntry),
	}
	go c.sweep()
	return c
}

func (c *Client) sweep() {
	t := time.NewTicker(10 * time.Minute)
	defer t.Stop()
	for range t.C {
		now := time.Now()
		c.mu.Lock()
		for k, e := range c.cache {
			if now.After(e.exp) {
				delete(c.cache, k)
			}
		}
		c.mu.Unlock()
	}
}

func keyPrefix(provider, debridKey string) string {
	sum := sha256.Sum256([]byte(provider + "\x00" + debridKey))
	return provider + ":" + hex.EncodeToString(sum[:6]) + ":"
}

func (c *Client) CheckCache(ctx context.Context, provider, debridKey string, hashes []string) map[string]bool {
	if c.apiKey == "" || debridKey == "" || len(hashes) == 0 {
		return nil
	}

	prefix := keyPrefix(provider, debridKey)
	result := make(map[string]bool)
	var miss []string
	now := time.Now()

	c.mu.Lock()
	for _, h := range hashes {
		lh := strings.ToLower(h)
		if e, ok := c.cache[prefix+lh]; ok && now.Before(e.exp) {
			result[lh] = e.cached
		} else {
			miss = append(miss, lh)
		}
	}
	c.mu.Unlock()

	if len(miss) == 0 {
		return result
	}

	fresh := c.query(ctx, provider, debridKey, miss)
	if fresh == nil {
		if len(result) == 0 {
			return nil
		}
		return result
	}

	exp := now.Add(cacheTTL)
	c.mu.Lock()
	for h, cached := range fresh {
		lh := strings.ToLower(h)
		c.cache[prefix+lh] = cacheEntry{cached: cached, exp: exp}
		result[lh] = cached
	}
	c.mu.Unlock()
	return result
}

func (c *Client) query(ctx context.Context, provider, debridKey string, hashes []string) map[string]bool {
	body, err := json.Marshal(map[string][]string{"infoHashes": hashes})
	if err != nil {
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, "POST", checkCacheURL, bytes.NewReader(body))
	if err != nil {
		return nil
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("X-Debrid-Provider", provider)
	req.Header.Set("X-Debrid-Key", debridKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", userAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		slog.Warn("torrentclaw: request failed", "provider", provider, "err", err)
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		slog.Warn("torrentclaw: non-200", "provider", provider, "status", resp.StatusCode)
		return nil
	}

	var data struct {
		Cached map[string]bool `json:"cached"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		slog.Warn("torrentclaw: decode failed", "provider", provider, "err", err)
		return nil
	}
	hits := 0
	for _, v := range data.Cached {
		if v {
			hits++
		}
	}
	slog.Info("torrentclaw: check ok", "provider", provider, "checked", len(hashes), "cached_hits", hits)
	return data.Cached
}
