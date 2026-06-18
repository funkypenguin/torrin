package poller

import (
	"context"
	"testing"

	"github.com/torrin-app/torrin/internal/jobs"
)

// failDownload must permanently fail a job on a genuine error, but on shutdown
// (cancelled context) leave it pending with a clean error so the next process
// resumes it instead of killing an interrupted download.
func TestFailDownload_ShutdownReclaimsInsteadOfFailing(t *testing.T) {
	store, err := jobs.NewStore(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	p := &Poller{store: store}

	job := &jobs.Job{ID: "j1", InfoHash: "h1", Status: jobs.StatusProcessing}
	if err := store.Create(job); err != nil {
		t.Fatal(err)
	}

	// Genuine failure (live context) -> failed + reason kept.
	p.failDownload(context.Background(), job, "download failed")
	got, _ := store.GetByID("j1")
	if got.Status != jobs.StatusFailed || got.Error != "download failed" {
		t.Fatalf("genuine failure: expected failed/'download failed', got %s / %q", got.Status, got.Error)
	}

	// Shutdown (cancelled context) -> pending + cleared error for re-drive.
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	job.Status = jobs.StatusProcessing
	p.failDownload(cctx, job, "download failed")
	got, _ = store.GetByID("j1")
	if got.Status != jobs.StatusPending || got.Error != "" {
		t.Fatalf("shutdown: expected pending/clean, got %s / %q", got.Status, got.Error)
	}
}
