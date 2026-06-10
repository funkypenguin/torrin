package premiumize

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const baseURL = "https://www.premiumize.me"

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

type CacheCheckResponse struct {
	Status   string   `json:"status"`
	Response []bool   `json:"response"`
	Filename []string `json:"filename"`
	Filesize []any    `json:"filesize"`
}

type DirectDLContent struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
	Link string `json:"link"`
}

type DirectDLResponse struct {
	Status  string            `json:"status"`
	Content []DirectDLContent `json:"content"`
}

type TransferResponse struct {
	Status string `json:"status"`
	ID     string `json:"id"`
	Name   string `json:"name"`
}

// CheckCache checks if magnets are cached. Returns true/false per item.
func (c *Client) CheckCache(ctx context.Context, magnets []string) ([]bool, error) {
	form := url.Values{}
	for _, m := range magnets {
		form.Add("items[]", m)
	}

	body, err := c.post(ctx, "/api/cache/check", form)
	if err != nil {
		return nil, fmt.Errorf("cache check: %w", err)
	}

	var resp CacheCheckResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("cache check: parse: %w", err)
	}
	if resp.Status != "success" {
		return nil, fmt.Errorf("cache check: %s", resp.Status)
	}
	return resp.Response, nil
}

func (c *Client) DirectDL(ctx context.Context, magnet string) (*DirectDLResponse, error) {
	form := url.Values{"src": {magnet}}

	body, err := c.post(ctx, "/api/transfer/directdl", form)
	if err != nil {
		return nil, fmt.Errorf("directdl: %w", err)
	}

	var resp DirectDLResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("directdl: parse: %w", err)
	}
	if resp.Status != "success" {
		return nil, fmt.Errorf("directdl: %s", resp.Status)
	}
	return &resp, nil
}

// DeleteTransfer removes a transfer by ID.
func (c *Client) DeleteTransfer(ctx context.Context, id string) error {
	form := url.Values{"id": {id}}
	_, err := c.post(ctx, "/api/transfer/delete", form)
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

func (c *Client) post(ctx context.Context, path string, form url.Values) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.URL.RawQuery = form.Encode()
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

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
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	return body, nil
}
