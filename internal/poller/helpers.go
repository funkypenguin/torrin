package poller

import (
	"path/filepath"
	"strings"
)

func isVideoFile(name string) bool {
	ext := strings.ToLower(filepath.Ext(name))
	switch ext {
	case ".mkv", ".mp4", ".avi", ".mov", ".wmv", ".flv", ".webm", ".m4v", ".ts":
		return true
	}
	return false
}

func contentTypeFor(ext string) string {
	switch strings.ToLower(ext) {
	case ".mkv":
		return "video/x-matroska"
	case ".mp4", ".m4v":
		return "video/mp4"
	case ".avi":
		return "video/x-msvideo"
	case ".webm":
		return "video/webm"
	default:
		return "application/octet-stream"
	}
}
