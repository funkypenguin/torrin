package usenet

import (
	"sync"
	"testing"

	"github.com/torrin-app/torrin/internal/usenet/postproc"
)

// TestDownloadSnapshotConcurrent reproduces the access pattern that
// previously raced: the manager's run goroutine mutates the lock-guarded
// fields while another goroutine (the poller) reads them. Reading through
// Snapshot must be race-free — run with `go test -race`.
func TestDownloadSnapshotConcurrent(t *testing.T) {
	dl := &Download{Status: StatusDownloading}

	const iters = 5000
	var wg sync.WaitGroup
	wg.Add(2)

	// Writer: mirrors the field writes in Manager.run / its progress callback.
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			dl.mu.Lock()
			dl.Progress = float64(i) / iters
			dl.Speed = int64(i)
			dl.Error = "downloading"
			if i == iters-1 {
				dl.Status = StatusComplete
				dl.Files = []postproc.OutputFile{{Name: "movie.mkv", Size: 1}}
			}
			dl.mu.Unlock()
		}
	}()

	// Reader: mirrors the poller, which only ever reads via Snapshot.
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			snap := dl.Snapshot()
			// Touch every field so the race detector observes the read.
			_ = snap.Status
			_ = snap.Progress
			_ = snap.Speed
			_ = snap.Error
			_ = snap.Files
		}
	}()

	wg.Wait()

	// The final snapshot must reflect the terminal state set under the lock.
	got := dl.Snapshot()
	if got.Status != StatusComplete {
		t.Fatalf("Status = %q, want %q", got.Status, StatusComplete)
	}
	if len(got.Files) != 1 || got.Files[0].Name != "movie.mkv" {
		t.Fatalf("Files = %+v, want one entry named movie.mkv", got.Files)
	}
}
