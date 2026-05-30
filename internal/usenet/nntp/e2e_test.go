package nntp_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/torrin-app/torrin/internal/usenet/assembler"
	"github.com/torrin-app/torrin/internal/usenet/nntp"
	"github.com/torrin-app/torrin/internal/usenet/nzb"
)

// TestLive_E2E_Download does a full end-to-end test: parse NZB, download segments,
// decode yEnc, assemble file. Requires NNTP_HOST/NNTP_USER/NNTP_PASS + a test NZB file.
// Run: NNTP_HOST=news.usenet.farm NNTP_USER=xxx NNTP_PASS=yyy go test -run TestLive_E2E -v
func TestLive_E2E_Download(t *testing.T) {
	skipIfNoLive(t)

	// Look for test NZB file.
	nzbPath := os.Getenv("NZB_FILE")
	if nzbPath == "" {
		nzbPath = "test_zip.nzb"
	}
	data, err := os.ReadFile(nzbPath)
	if err != nil {
		// Try from project root.
		data, err = os.ReadFile(filepath.Join("..", "..", "..", "..", nzbPath))
		if err != nil {
			t.Skipf("no NZB file found at %s (set NZB_FILE env)", nzbPath)
		}
	}

	// Parse NZB.
	parsed, err := nzb.ParseBytes(data)
	if err != nil {
		t.Fatalf("parse NZB: %v", err)
	}
	t.Logf("NZB: %d files, %d total bytes", len(parsed.Files), parsed.TotalSize())

	hash := nzb.Hash(parsed)
	t.Logf("content hash: %s", hash)

	// Create pool.
	pool := nntp.NewPool(liveCreds())
	defer pool.Close()

	// Select group from first file.
	if len(parsed.Files) > 0 && len(parsed.Files[0].Groups) > 0 {
		c, err := pool.Get()
		if err != nil {
			t.Fatal(err)
		}
		c.Group(parsed.Files[0].Groups[0])
		pool.Put(c)
		t.Logf("selected group: %s", parsed.Files[0].Groups[0])
	}

	// Download.
	tmpDir := t.TempDir()
	asm := assembler.New(pool)

	results, err := asm.DownloadAll(context.Background(), parsed, tmpDir, func(progress float64, bytes int64) {
		if int(progress*100)%25 == 0 {
			t.Logf("progress: %.0f%% (%d bytes)", progress*100, bytes)
		}
	})
	if err != nil {
		t.Fatalf("download failed: %v", err)
	}

	t.Logf("downloaded %d files:", len(results))
	for _, r := range results {
		t.Logf("  %s (%d bytes) -> %s", r.Name, r.Size, r.Path)

		// Verify file exists and has content.
		info, err := os.Stat(r.Path)
		if err != nil {
			t.Fatalf("file not found: %s", r.Path)
		}
		if info.Size() == 0 {
			t.Fatalf("file is empty: %s", r.Path)
		}
	}
}
