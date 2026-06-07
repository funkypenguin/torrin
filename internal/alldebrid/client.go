package alldebrid

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const baseURL = "https://api.alldebrid.com/v4"

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

func NewClientWithProxy(apiKey string, proxyURL *url.URL) *Client {
	transport := &http.Transport{Proxy: http.ProxyURL(proxyURL)}
	return &Client{
		apiKey:     apiKey,
		httpClient: &http.Client{Timeout: 30 * time.Second, Transport: transport},
		dlClient:   &http.Client{Timeout: 0, Transport: transport},
	}
}

type UnlockResponse struct {
	Link     string `json:"link"`
	Host     string `json:"host"`
	Filename string `json:"filename"`
	FileSize int64  `json:"filesize"`
	ID       string `json:"id"`
}

type apiResponse struct {
	Status string          `json:"status"`
	Data   json.RawMessage `json:"data"`
	Error  *apiError       `json:"error"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type MagnetUploadResponse struct {
	ID    int    `json:"id"`
	Ready bool   `json:"ready"`
	Name  string `json:"name"`
	Size  int64  `json:"size"`
	Hash  string `json:"hash"`
}

type MagnetFile struct {
	Name string       `json:"n"`
	Size int64        `json:"s"`
	Link string       `json:"l"`
	Sub  []MagnetFile `json:"e"`
}

type MagnetStatus struct {
	ID         int    `json:"id"`
	Filename   string `json:"filename"`
	Size       int64  `json:"size"`
	StatusCode int    `json:"statusCode"`
	Downloaded int64  `json:"downloaded"`
	Seeders    int    `json:"seeders"`
}

func (c *Client) AddMagnet(ctx context.Context, magnet string) (*MagnetUploadResponse, error) {
	form := url.Values{"magnets[]": {magnet}}
	req, err := http.NewRequestWithContext(ctx, "POST", baseURL+"/magnet/upload", nil)
	if err != nil {
		return nil, err
	}
	req.URL.RawQuery = form.Encode()
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	body, err := c.doRequest(req)
	if err != nil {
		return nil, fmt.Errorf("add magnet: %w", err)
	}

	var data struct {
		Magnets []MagnetUploadResponse `json:"magnets"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, fmt.Errorf("add magnet: parse: %w", err)
	}
	if len(data.Magnets) == 0 {
		return nil, fmt.Errorf("add magnet: no result")
	}
	return &data.Magnets[0], nil
}

func (c *Client) GetMagnetFiles(ctx context.Context, magnetID int) ([]MagnetFile, error) {
	form := url.Values{"id[]": {fmt.Sprintf("%d", magnetID)}}
	req, err := http.NewRequestWithContext(ctx, "POST", baseURL+"/magnet/files", nil)
	if err != nil {
		return nil, err
	}
	req.URL.RawQuery = form.Encode()
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	body, err := c.doRequest(req)
	if err != nil {
		return nil, fmt.Errorf("magnet files: %w", err)
	}

	var data struct {
		Magnets []struct {
			ID    string       `json:"id"`
			Files []MagnetFile `json:"files"`
		} `json:"magnets"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, fmt.Errorf("magnet files: parse: %w", err)
	}
	if len(data.Magnets) == 0 {
		return nil, fmt.Errorf("magnet files: no result")
	}

	return flattenFiles(data.Magnets[0].Files), nil
}

func (c *Client) DeleteMagnet(ctx context.Context, magnetID int) error {
	form := url.Values{"id": {fmt.Sprintf("%d", magnetID)}}
	req, err := http.NewRequestWithContext(ctx, "POST", baseURL+"/magnet/delete", nil)
	if err != nil {
		return err
	}
	req.URL.RawQuery = form.Encode()
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	_, err = c.doRequest(req)
	return err
}

func flattenFiles(files []MagnetFile) []MagnetFile {
	var result []MagnetFile
	for _, f := range files {
		if f.Link != "" {
			result = append(result, f)
		}
		if len(f.Sub) > 0 {
			result = append(result, flattenFiles(f.Sub)...)
		}
	}
	return result
}

func (c *Client) doRequest(req *http.Request) (json.RawMessage, error) {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	var apiResp apiResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}

	if apiResp.Status != "success" {
		if apiResp.Error != nil {
			return nil, fmt.Errorf("%s: %s", apiResp.Error.Code, apiResp.Error.Message)
		}
		return nil, fmt.Errorf("failed: %s", string(body))
	}

	return apiResp.Data, nil
}

func (c *Client) Unlock(ctx context.Context, link string) (*UnlockResponse, error) {
	form := url.Values{"link": {link}}
	req, err := http.NewRequestWithContext(ctx, "POST", baseURL+"/link/unlock", nil)
	if err != nil {
		return nil, err
	}
	req.URL.RawQuery = form.Encode()
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	body, err := c.doRequest(req)
	if err != nil {
		return nil, fmt.Errorf("unlock: %w", err)
	}

	var result UnlockResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("unlock: parse: %w", err)
	}
	return &result, nil
}

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
