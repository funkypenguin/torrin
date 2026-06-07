package realdebrid

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
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
	mu     sync.Mutex
	tokens int
	max    int
	ticker *time.Ticker
}

func newRateLimiter(maxPerInterval int, interval time.Duration) *rateLimiter {
	rl := &rateLimiter{
		tokens: maxPerInterval,
		max:    maxPerInterval,
		ticker: time.NewTicker(interval / time.Duration(maxPerInterval)),
	}
	go func() {
		for range rl.ticker.C {
			rl.mu.Lock()
			if rl.tokens < rl.max {
				rl.tokens++
			}
			rl.mu.Unlock()
		}
	}()
	return rl
}

func (rl *rateLimiter) wait(ctx context.Context) {
	for {
		rl.mu.Lock()
		if rl.tokens > 0 {
			rl.tokens--
			rl.mu.Unlock()
			return
		}
		rl.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
}
