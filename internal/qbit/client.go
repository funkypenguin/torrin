package qbit

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"time"
)

type Client struct {
	baseURL  string
	username string
	password string
	http     *http.Client
}

type Torrent struct {
	Hash        string  `json:"hash"`
	Name        string  `json:"name"`
	Size        int64   `json:"size"`
	Progress    float64 `json:"progress"`
	DlSpeed     int64   `json:"dlspeed"`
	State       string  `json:"state"`
	SavePath    string  `json:"save_path"`
	ContentPath string  `json:"content_path"`
	Category    string  `json:"category"`
	ETA         int64   `json:"eta"`
}

type TorrentFile struct {
	Index    int     `json:"index"`
	Name     string  `json:"name"`
	Size     int64   `json:"size"`
	Priority int     `json:"priority"`
	Progress float64 `json:"progress"`
}

func NewClient(baseURL, username, password string) *Client {
	jar, _ := cookiejar.New(nil)
	return &Client{
		baseURL:  strings.TrimRight(baseURL, "/"),
		username: username,
		password: password,
		http: &http.Client{
			Jar:     jar,
			Timeout: 30 * time.Second,
		},
	}
}

func (c *Client) Login() error {
	resp, err := c.http.PostForm(c.baseURL+"/api/v2/auth/login", url.Values{
		"username": {c.username},
		"password": {c.password},
	})
	if err != nil {
		return fmt.Errorf("login: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == 200 || resp.StatusCode == 204 {
		return nil
	}
	return fmt.Errorf("login failed: %s (status %d)", body, resp.StatusCode)
}

func (c *Client) AddMagnet(magnet string) error {
	data := url.Values{
		"urls":               {magnet},
		"savepath":           {"/downloads"},
		"category":           {"torrin"},
		"sequentialDownload": {"true"},
		"firstLastPiecePrio": {"true"},
		"stopCondition":      {"MetadataReceived"},
	}

	resp, err := c.http.PostForm(c.baseURL+"/api/v2/torrents/add", data)
	if err != nil {
		return fmt.Errorf("add magnet: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("add magnet (%d): %s", resp.StatusCode, body)
	}
	return nil
}

func (c *Client) GetTorrent(hash string) (*Torrent, error) {
	resp, err := c.http.Get(c.baseURL + "/api/v2/torrents/info?hashes=" + strings.ToLower(hash))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var torrents []Torrent
	if err := json.NewDecoder(resp.Body).Decode(&torrents); err != nil {
		return nil, err
	}
	if len(torrents) == 0 {
		return nil, fmt.Errorf("torrent %s not found", hash)
	}
	return &torrents[0], nil
}

func (c *Client) GetFiles(hash string) ([]TorrentFile, error) {
	resp, err := c.http.Get(c.baseURL + "/api/v2/torrents/files?hash=" + strings.ToLower(hash))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var files []TorrentFile
	if err := json.NewDecoder(resp.Body).Decode(&files); err != nil {
		return nil, err
	}
	for i := range files {
		files[i].Index = i
	}
	return files, nil
}

func (c *Client) SetFilePriority(hash string, fileIndexes []int, priority int) error {
	idxStrs := make([]string, len(fileIndexes))
	for i, idx := range fileIndexes {
		idxStrs[i] = fmt.Sprintf("%d", idx)
	}
	resp, err := c.http.PostForm(c.baseURL+"/api/v2/torrents/filePrio", url.Values{
		"hash":     {strings.ToLower(hash)},
		"id":       {strings.Join(idxStrs, "|")},
		"priority": {fmt.Sprintf("%d", priority)},
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func (c *Client) Resume(hash string) error {
	data := url.Values{"hashes": {strings.ToLower(hash)}}
	resp, err := c.http.PostForm(c.baseURL+"/api/v2/torrents/start", data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		resp2, err := c.http.PostForm(c.baseURL+"/api/v2/torrents/resume", data)
		if err != nil {
			return err
		}
		defer resp2.Body.Close()
	}
	return nil
}

func (c *Client) Reannounce(hash string) error {
	resp, err := c.http.PostForm(c.baseURL+"/api/v2/torrents/reannounce", url.Values{
		"hashes": {strings.ToLower(hash)},
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func IsStalled(t *Torrent) bool {
	return t.State == "stalledDL" && t.DlSpeed == 0
}

func IsFetchingMetadata(t *Torrent) bool {
	return t.State == "metaDL"
}

func (c *Client) Delete(hash string) error {
	resp, err := c.http.PostForm(c.baseURL+"/api/v2/torrents/delete", url.Values{
		"hashes":      {strings.ToLower(hash)},
		"deleteFiles": {"true"},
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete (%d): %s", resp.StatusCode, body)
	}
	return nil
}

func IsComplete(t *Torrent) bool {
	switch t.State {
	case "uploading", "stalledUP", "pausedUP", "forcedUP", "checkingUP", "queuedUP":
		return true
	case "stoppedUP":
		return true
	}
	return t.Progress >= 1.0
}

func IsDownloading(t *Torrent) bool {
	switch t.State {
	case "downloading", "stalledDL", "forcedDL", "metaDL", "allocating":
		return true
	}
	return false
}

func IsError(t *Torrent) bool {
	return t.State == "error" || t.State == "missingFiles"
}
