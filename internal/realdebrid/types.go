package realdebrid

import "time"

// AvailabilityResponse represents the instant availability response.
// Structure: map[fileHash] -> []map[fileID]FileInfo
type AvailabilityResponse map[string][]map[string]FileAvail

type FileAvail struct {
	Filename string `json:"filename"`
	Filesize int64  `json:"filesize"`
}

type Torrent struct {
	ID       string        `json:"id"`
	Hash     string        `json:"hash"`
	Filename string        `json:"filename"`
	Bytes    int64         `json:"bytes"`
	Status   string        `json:"status"`
	Progress float64       `json:"progress"`
	Links    []string      `json:"links"`
	Files    []TorrentFile `json:"files"`
	Added    time.Time     `json:"added"`
	Speed    int64         `json:"speed"`
}

type TorrentFile struct {
	ID       int    `json:"id"`
	Path     string `json:"path"`
	Bytes    int64  `json:"bytes"`
	Selected int    `json:"selected"`
}

type AddMagnetResponse struct {
	ID  string `json:"id"`
	URI string `json:"uri"`
}

type UnrestrictResponse struct {
	ID       string `json:"id"`
	Filename string `json:"filename"`
	FileSize int64  `json:"filesize"`
	Download string `json:"download"`
	MimeType string `json:"mimeType"`
}

type APIError struct {
	ErrorCode int    `json:"error_code"`
	ErrorMsg  string `json:"error"`
}

func (e *APIError) Error() string {
	return e.ErrorMsg
}
