package poller

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/torrin-app/torrin/internal/jobs"
)

// maxDownloadAttempts bounds how many times a stalled/dropped transfer is resumed
// before the download is declared failed.
const maxDownloadAttempts = 6

// downloadBackoff is the wait between resume attempts; a package var so tests can
// stub out the real sleeps.
var downloadBackoff = sleepBackoff

// rangeOpener opens downloadURL starting at offset bytes. It returns the body, the
// TOTAL file size (0 if unknown), and full=true when the server ignored the Range
// header and returned the whole file from byte 0 (so the caller restarts from 0).
type rangeOpener func(offset int64) (body io.ReadCloser, total int64, full bool, err error)

// downloadResilient streams from open() into f, resuming with HTTP Range requests
// when the connection drops mid-transfer (the "unexpected EOF" case). It retries up
// to maxDownloadAttempts with backoff, and only reports success once the body ends
// cleanly AND the bytes written match the known total (so silent truncation fails
// instead of passing). onProgress, if set, is called roughly every 2s with the
// bytes written so far and the total.
func downloadResilient(ctx context.Context, open rangeOpener, f *os.File, onProgress func(written, total int64)) error {
	var written, total int64
	var lastErr error

	if fi, err := f.Stat(); err == nil && fi.Size() > 0 {
		written = fi.Size()
	}

	for attempt := 0; attempt < maxDownloadAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		body, sz, full, err := open(written)
		if err != nil {
			lastErr = err
			downloadBackoff(ctx, attempt)
			continue
		}

		if full && written > 0 {
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				body.Close()
				return err
			}
			if err := f.Truncate(0); err != nil {
				body.Close()
				return err
			}
			written = 0
		} else if written > 0 {
			if _, err := f.Seek(written, io.SeekStart); err != nil {
				body.Close()
				return err
			}
		}
		if sz > 0 {
			total = sz
		}

		n, copyErr, fatal := copyBody(ctx, body, f, written, total, onProgress)
		body.Close()
		written = n

		if fatal {
			return copyErr
		}
		if copyErr == nil {
			if total > 0 && written < total {
				lastErr = fmt.Errorf("truncated: got %d of %d bytes", written, total)
				downloadBackoff(ctx, attempt)
				continue
			}
			return nil
		}

		lastErr = copyErr
		downloadBackoff(ctx, attempt)
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("download failed after %d attempts", maxDownloadAttempts)
	}
	return lastErr
}

func (p *Poller) downloadToFile(ctx context.Context, open rangeOpener, localPath string, job *jobs.Job, totalSize int64, fileIdx, fileCount int) error {
	if totalSize > 0 {
		if fi, err := os.Stat(localPath); err == nil && fi.Size() >= totalSize {
			return nil
		}
	}
	f, err := os.OpenFile(localPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	lastUpdate := time.Now()
	lastBytes := int64(0)
	onProgress := func(written, total int64) {
		elapsed := time.Since(lastUpdate).Seconds()
		if elapsed <= 0 {
			return
		}
		speed := float64(written-lastBytes) / elapsed
		t := total
		if t <= 0 {
			t = totalSize
		}
		filePct := 0.0
		if t > 0 {
			filePct = float64(written) / float64(t)
		}
		overallPct := int((float64(fileIdx) + filePct) / float64(fileCount) * 100)
		var msg string
		if fileCount > 1 {
			msg = fmt.Sprintf("downloading — %d%% (%d/%d, %d B/s)", overallPct, fileIdx+1, fileCount, int64(speed))
		} else {
			msg = fmt.Sprintf("downloading — %d%% (%d B/s)", overallPct, int64(speed))
		}
		if job.Error != msg {
			job.Error = msg
			p.store.Update(job)
		}
		lastUpdate = time.Now()
		lastBytes = written
	}

	if err := downloadResilient(ctx, open, f, onProgress); err != nil {
		if ctx.Err() == nil {
			os.Remove(localPath)
		}
		return err
	}
	return nil
}

func copyBody(ctx context.Context, body io.Reader, f io.Writer, written, total int64, onProgress func(written, total int64)) (int64, error, bool) {
	buf := make([]byte, 256*1024)
	lastUpdate := time.Now()

	for {
		if ctx.Err() != nil {
			return written, ctx.Err(), true
		}
		n, readErr := body.Read(buf)
		if n > 0 {
			if _, wErr := f.Write(buf[:n]); wErr != nil {
				return written, wErr, true
			}
			written += int64(n)
			if onProgress != nil && time.Since(lastUpdate) >= 2*time.Second {
				onProgress(written, total)
				lastUpdate = time.Now()
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				return written, nil, false
			}
			return written, readErr, false
		}
	}
}

func sleepBackoff(ctx context.Context, attempt int) {
	d := time.Duration(1<<uint(attempt)) * time.Second
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
