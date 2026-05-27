package jobs

import "time"

type Status string

const (
	StatusPending    Status = "pending"    // Magnet submitted, waiting for qBittorrent
	StatusQueued     Status = "queued"     // Waiting for disk budget
	StatusMetadata   Status = "metadata"   // Metadata received (kept for RD API compat)
	StatusProcessing Status = "processing" // Downloading in qBittorrent
	StatusComplete   Status = "complete"   // Done — streaming URLs available
	StatusFailed     Status = "failed"     // Terminal failure
	StatusCached     Status = "cached"     // Already in R2, instant response
)

type Job struct {
	ID           string    `json:"id"`
	UserID       string    `json:"user_id,omitempty"`
	InfoHash     string    `json:"info_hash"`
	Name         string    `json:"name"`
	Magnet       string    `json:"magnet,omitempty"`
	Status       Status    `json:"status"`
	Error        string    `json:"error,omitempty"`
	Files        []File    `json:"files,omitempty"`
	SelectedIdxs []int     `json:"selected_indexes,omitempty"`
	StreamURLs   []Stream  `json:"stream_urls,omitempty"`
	FileSize     int64     `json:"file_size"`
	MaxBytes     int64     `json:"max_bytes"`
	Priority     int       `json:"priority"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type File struct {
	Index int    `json:"index"`
	Name  string `json:"name"`
	Size  int64  `json:"size"`
}

type Stream struct {
	FileName  string `json:"file_name"`
	DirectURL string `json:"direct_url,omitempty"`
	SignedURL string `json:"signed_url,omitempty"`
}
