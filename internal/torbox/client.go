package torbox

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.torbox.app/v1/api"

type Client struct {
	apiKey     string
	httpClient *http.Client
	dlClient   *http.Client
}

func NewClient(apiKey string) *Client {
	return &Client{
		apiKey:     apiKey,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		dlClient:   &http.Client{Timeout: 0},
	}
}

func (c *Client) APIKey() string {
	return c.apiKey
}

type CacheResponse struct {
	Success bool          `json:"success"`
	Data    []CacheResult `json:"data"`
}

type CacheResult struct {
	Hash string `json:"hash"`
	Name string `json:"name"`
	Size int64  `json:"size"`
}

type CreateTorrentResponse struct {
	Success bool `json:"success"`
	Data    struct {
		TorrentID int    `json:"torrent_id"`
		Name      string `json:"name"`
		Hash      string `json:"hash"`
	} `json:"data"`
}

type DownloadLinkResponse struct {
	Success bool   `json:"success"`
	Data    string `json:"data"`
}

type apiError struct {
	Success bool   `json:"success"`
	Detail  string `json:"detail"`
	Error   string `json:"error"`
}

type TorrentListItem struct {
	ID     int    `json:"id"`
	Hash   string `json:"hash"`
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	Status string `json:"download_state"`
}

func (c *Client) ListTorrents(ctx context.Context, offset, limit int) ([]TorrentListItem, error) {
	path := fmt.Sprintf("%s/torrents/mylist?offset=%d&limit=%d", baseURL, offset, limit)
	req, err := http.NewRequestWithContext(ctx, "GET", path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	body, err := c.doRequest(req)
	if err != nil {
		return nil, fmt.Errorf("list torrents: %w", err)
	}

	var resp struct {
		Success bool              `json:"success"`
		Data    []TorrentListItem `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("list torrents: parse: %w", err)
	}
	return resp.Data, nil
}

// CheckCached checks if hashes are cached on TorBox.
func (c *Client) CheckCached(ctx context.Context, hashes []string) ([]CacheResult, error) {
	hashParam := strings.Join(hashes, ",")
	req, err := http.NewRequestWithContext(ctx, "GET", baseURL+"/torrents/checkcached?hash="+url.QueryEscape(hashParam)+"&format=list", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	body, err := c.doRequest(req)
	if err != nil {
		return nil, fmt.Errorf("check cached: %w", err)
	}

	var resp struct {
		Success bool          `json:"success"`
		Data    []CacheResult `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("check cached: parse: %w", err)
	}
	if !resp.Success {
		return nil, fmt.Errorf("check cached: failed")
	}
	return resp.Data, nil
}

// CreateTorrent adds a magnet link.
func (c *Client) CreateTorrent(ctx context.Context, magnet string) (*CreateTorrentResponse, error) {
	form := url.Values{"magnet": {magnet}}
	req, err := http.NewRequestWithContext(ctx, "POST", baseURL+"/torrents/createtorrent", strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	body, err := c.doRequest(req)
	if err != nil {
		return nil, fmt.Errorf("create torrent: %w", err)
	}

	var resp CreateTorrentResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("create torrent: parse: %w", err)
	}
	if !resp.Success {
		return nil, fmt.Errorf("create torrent: failed")
	}
	return &resp, nil
}

// RequestDownloadLink gets a download link for a torrent file.
func (c *Client) RequestDownloadLink(ctx context.Context, torrentID int, fileID int) (string, error) {
	path := fmt.Sprintf("%s/torrents/requestdl?token=%s&torrent_id=%d&file_id=%d",
		baseURL, url.QueryEscape(c.apiKey), torrentID, fileID)

	req, err := http.NewRequestWithContext(ctx, "GET", path, nil)
	if err != nil {
		return "", err
	}

	body, err := c.doRequest(req)
	if err != nil {
		return "", fmt.Errorf("request dl: %w", err)
	}

	var resp DownloadLinkResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", fmt.Errorf("request dl: parse: %w", err)
	}
	if !resp.Success {
		return "", fmt.Errorf("request dl: failed")
	}
	return resp.Data, nil
}

// RequestDownloadLinkWithRetry requests a download link and verifies the CDN is reachable.
// Retries up to 3 times with new links in case the CDN node is down.
func (c *Client) RequestDownloadLinkWithRetry(ctx context.Context, torrentID int, fileID int) (string, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		dlURL, err := c.RequestDownloadLink(ctx, torrentID, fileID)
		if err != nil {
			lastErr = err
			continue
		}
		testReq, _ := http.NewRequestWithContext(ctx, "HEAD", dlURL, nil)
		resp, err := c.dlClient.Do(testReq)
		if err != nil {
			lastErr = fmt.Errorf("cdn unreachable (attempt %d): %w", attempt+1, err)
			time.Sleep(2 * time.Second)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode >= 400 {
			lastErr = fmt.Errorf("cdn returned %d (attempt %d)", resp.StatusCode, attempt+1)
			time.Sleep(2 * time.Second)
			continue
		}
		return dlURL, nil
	}
	return "", fmt.Errorf("cdn unreachable after retries: %w", lastErr)
}

// DeleteTorrent removes a torrent.
func (c *Client) DeleteTorrent(ctx context.Context, torrentID int) error {
	payload := fmt.Sprintf(`{"torrent_id":%d,"operation":"delete"}`, torrentID)
	req, err := http.NewRequestWithContext(ctx, "POST", baseURL+"/torrents/controltorrent", strings.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	_, err = c.doRequest(req)
	return err
}

// DownloadFile downloads from a direct URL.
func (c *Client) DownloadFile(ctx context.Context, downloadURL string) (io.ReadCloser, int64, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", downloadURL, nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := c.dlClient.Do(req)
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
	resp, err := c.dlClient.Do(req)
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

func (c *Client) doRequest(req *http.Request) ([]byte, error) {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	if resp.StatusCode >= 400 {
		var apiErr apiError
		if json.Unmarshal(body, &apiErr) == nil && apiErr.Detail != "" {
			return nil, fmt.Errorf("%s", apiErr.Detail)
		}
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	return body, nil
}
