package realdebrid

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

func NewClientWithProxy(apiKey string, proxyURL *url.URL) *Client {
	transport := &http.Transport{Proxy: http.ProxyURL(proxyURL)}
	return &Client{
		apiKey:         apiKey,
		httpClient:     &http.Client{Timeout: 30 * time.Second, Transport: transport},
		downloadClient: &http.Client{Timeout: 0, Transport: transport},
		limiter:        newRateLimiter(250, time.Minute),
		log:            slog.Default().With("pkg", "realdebrid"),
	}
}

const baseURL = "https://api.real-debrid.com/rest/1.0"

type Client struct {
	apiKey         string
	httpClient     *http.Client
	downloadClient *http.Client
	limiter        *rateLimiter
	log            *slog.Logger
}

func NewClient(apiKey string) *Client {
	return &Client{
		apiKey:         apiKey,
		httpClient:     &http.Client{Timeout: 30 * time.Second},
		downloadClient: &http.Client{Timeout: 0},
		limiter:        newRateLimiter(250, time.Minute),
		log:            slog.Default().With("pkg", "realdebrid"),
	}
}

func (c *Client) APIKey() string {
	return c.apiKey
}

func (c *Client) AddMagnet(ctx context.Context, magnet string) (*AddMagnetResponse, error) {
	var resp AddMagnetResponse
	form := url.Values{"magnet": {magnet}}
	if err := c.post(ctx, "/torrents/addMagnet", form, &resp); err != nil {
		return nil, fmt.Errorf("add magnet: %w", err)
	}
	return &resp, nil
}

func (c *Client) SelectFiles(ctx context.Context, torrentID string, fileIDs string) error {
	form := url.Values{"files": {fileIDs}}
	return c.post(ctx, "/torrents/selectFiles/"+torrentID, form, nil)
}

func (c *Client) GetTorrent(ctx context.Context, torrentID string) (*Torrent, error) {
	var resp Torrent
	if err := c.get(ctx, "/torrents/info/"+torrentID, &resp); err != nil {
		return nil, fmt.Errorf("torrent info: %w", err)
	}
	return &resp, nil
}

func (c *Client) Unrestrict(ctx context.Context, link string) (*UnrestrictResponse, error) {
	var resp UnrestrictResponse
	form := url.Values{"link": {link}}
	if err := c.post(ctx, "/unrestrict/link", form, &resp); err != nil {
		return nil, fmt.Errorf("unrestrict: %w", err)
	}
	return &resp, nil
}

func (c *Client) ListTorrents(ctx context.Context, page, limit int) ([]Torrent, error) {
	var resp []Torrent
	path := fmt.Sprintf("/torrents?page=%d&limit=%d", page, limit)
	if err := c.get(ctx, path, &resp); err != nil {
		return nil, fmt.Errorf("list torrents: %w", err)
	}
	return resp, nil
}

func (c *Client) DeleteTorrent(ctx context.Context, torrentID string) error {
	return c.del(ctx, "/torrents/delete/"+torrentID)
}

func (c *Client) DownloadFile(ctx context.Context, downloadURL string) (io.ReadCloser, int64, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", downloadURL, nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := c.downloadClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		return nil, 0, fmt.Errorf("download: HTTP %d", resp.StatusCode)
	}
	return resp.Body, resp.ContentLength, nil
}

func (c *Client) DownloadFileRange(ctx context.Context, downloadURL string, offset int64) (body io.ReadCloser, total int64, full bool, err error) {
	req, err := http.NewRequestWithContext(ctx, "GET", downloadURL, nil)
	if err != nil {
		return nil, 0, false, err
	}
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	resp, err := c.downloadClient.Do(req)
	if err != nil {
		return nil, 0, false, err
	}
	switch resp.StatusCode {
	case http.StatusOK:
		return resp.Body, resp.ContentLength, true, nil
	case http.StatusPartialContent:
		return resp.Body, totalFromContentRange(resp.Header.Get("Content-Range")), false, nil
	default:
		resp.Body.Close()
		return nil, 0, false, fmt.Errorf("download: HTTP %d", resp.StatusCode)
	}
}

func totalFromContentRange(cr string) int64 {
	i := strings.LastIndex(cr, "/")
	if i < 0 || i+1 >= len(cr) {
		return 0
	}
	total, err := strconv.ParseInt(strings.TrimSpace(cr[i+1:]), 10, 64)
	if err != nil {
		return 0
	}
	return total
}

func (c *Client) get(ctx context.Context, path string, result interface{}) error {
	c.limiter.wait(ctx)
	req, err := http.NewRequestWithContext(ctx, "GET", baseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	return c.doJSON(req, result)
}

func (c *Client) post(ctx context.Context, path string, form url.Values, result interface{}) error {
	c.limiter.wait(ctx)
	req, err := http.NewRequestWithContext(ctx, "POST", baseURL+path, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return c.doJSON(req, result)
}

func (c *Client) del(ctx context.Context, path string) error {
	c.limiter.wait(ctx)
	req, err := http.NewRequestWithContext(ctx, "DELETE", baseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func (c *Client) doJSON(req *http.Request, result interface{}) error {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode >= 400 {
		var apiErr APIError
		if json.Unmarshal(body, &apiErr) == nil && apiErr.ErrorMsg != "" {
			return &apiErr
		}
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	if result != nil && len(body) > 0 {
		return json.Unmarshal(body, result)
	}
	return nil
}

type rateLimiter struct {
	mu       sync.Mutex
	tokens   float64
	max      float64
	rate     float64
	lastTime time.Time
}

func newRateLimiter(maxPerInterval int, interval time.Duration) *rateLimiter {
	return &rateLimiter{
		tokens:   float64(maxPerInterval),
		max:      float64(maxPerInterval),
		rate:     float64(maxPerInterval) / float64(interval),
		lastTime: time.Now(),
	}
}

func (rl *rateLimiter) wait(ctx context.Context) {
	for {
		rl.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(rl.lastTime)
		rl.tokens += float64(elapsed) * rl.rate
		if rl.tokens > rl.max {
			rl.tokens = rl.max
		}
		rl.lastTime = now
		if rl.tokens >= 1 {
			rl.tokens--
			rl.mu.Unlock()
			return
		}
		rl.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}
