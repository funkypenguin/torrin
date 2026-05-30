package usenet

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/torrin-app/torrin/internal/usenet/assembler"
	"github.com/torrin-app/torrin/internal/usenet/nntp"
	"github.com/torrin-app/torrin/internal/usenet/nzb"
	"github.com/torrin-app/torrin/internal/usenet/postproc"
)

type OutputFile = postproc.OutputFile

type DownloadStatus string

const (
	StatusDownloading    DownloadStatus = "downloading"
	StatusPostProcessing DownloadStatus = "postprocessing"
	StatusComplete       DownloadStatus = "complete"
	StatusFailed         DownloadStatus = "failed"
)

type Download struct {
	NZBHash   string
	NZBName   string
	UserID    string
	Status    DownloadStatus
	Progress  float64
	Speed     int64
	Error     string
	OutputDir string
	Files     []postproc.OutputFile
	StartedAt time.Time

	mu             sync.Mutex
	lastBytes      int64
	lastProgressAt time.Time
	lastSpeedCalc  time.Time
	cancel         context.CancelFunc
}

type Manager struct {
	credProvider CredentialProvider
	pools        sync.Map
	downloads    sync.Map
	downloadDir  string
	stopCleanup  chan struct{}
}

func NewManager(credProvider CredentialProvider, downloadDir string) *Manager {
	m := &Manager{
		credProvider: credProvider,
		downloadDir:  downloadDir,
		stopCleanup:  make(chan struct{}),
	}
	go m.cleanupLoop()
	return m
}

func (m *Manager) Submit(ctx context.Context, userID string, nzbData []byte, jobName ...string) (string, error) {
	parsed, err := nzb.ParseBytes(nzbData)
	if err != nil {
		return "", fmt.Errorf("parse nzb: %w", err)
	}

	hash := nzb.Hash(parsed)

	if _, loaded := m.downloads.Load(hash); loaded {
		return hash, nil
	}

	pool, err := m.getPool(ctx, userID)
	if err != nil {
		return "", fmt.Errorf("get pool: %w", err)
	}

	dlCtx, cancel := context.WithCancel(ctx)
	now := time.Now()
	dlName := parsed.Name()
	if len(jobName) > 0 && jobName[0] != "" {
		dlName = jobName[0]
	}
	dl := &Download{
		NZBHash:        hash,
		NZBName:        dlName,
		UserID:         userID,
		Status:         StatusDownloading,
		OutputDir:      filepath.Join(m.downloadDir, hash),
		StartedAt:      now,
		cancel:         cancel,
		lastProgressAt: now,
		lastSpeedCalc:  now,
	}
	m.downloads.Store(hash, dl)

	go m.run(dlCtx, dl, parsed, pool)

	return hash, nil
}

func (m *Manager) GetDownload(nzbHash string) *Download {
	if v, ok := m.downloads.Load(nzbHash); ok {
		return v.(*Download)
	}
	return nil
}

func (m *Manager) Cancel(nzbHash string) error {
	v, ok := m.downloads.Load(nzbHash)
	if !ok {
		return fmt.Errorf("download %s not found", nzbHash)
	}
	dl := v.(*Download)
	if dl.cancel != nil {
		dl.cancel()
	}
	dl.mu.Lock()
	dl.Status = StatusFailed
	dl.Error = "cancelled"
	dl.mu.Unlock()
	return nil
}

func (m *Manager) CleanupFiles(nzbHash string) error {
	v, ok := m.downloads.Load(nzbHash)
	if !ok {
		return nil
	}
	dl := v.(*Download)
	m.downloads.Delete(nzbHash)
	if dl.OutputDir != "" {
		return os.RemoveAll(dl.OutputDir)
	}
	return nil
}

func (m *Manager) TestCredentials(ctx context.Context, creds *nntp.Credentials) error {
	conn, err := nntp.Dial(creds)
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}

func (m *Manager) run(ctx context.Context, dl *Download, parsed *nzb.NZB, pool *nntp.Pool) {
	slog.Info("usenet download started", "hash", dl.NZBHash, "files", len(parsed.Files))

	// Stall timeout: cancel if no progress for 30 minutes.
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				dl.mu.Lock()
				stalled := time.Since(dl.lastProgressAt) > 30*time.Minute
				status := dl.Status
				dl.mu.Unlock()
				if stalled && status == StatusDownloading {
					slog.Warn("usenet download stalled, cancelling", "hash", dl.NZBHash)
					dl.mu.Lock()
					dl.Status = StatusFailed
					dl.Error = "download stalled — no progress for 30 minutes"
					dl.mu.Unlock()
					dl.cancel()
					return
				}
			}
		}
	}()

	// Download all segments.
	asm := assembler.New(pool)
	results, err := asm.DownloadAll(ctx, parsed, dl.OutputDir, func(progress float64, bytes int64) {
		dl.mu.Lock()
		dl.Progress = progress
		dl.lastProgressAt = time.Now()
		// Calculate speed.
		now := time.Now()
		elapsed := now.Sub(dl.lastSpeedCalc).Seconds()
		if elapsed >= 1.0 {
			dl.Speed = int64(float64(bytes-dl.lastBytes) / elapsed)
			dl.lastBytes = bytes
			dl.lastSpeedCalc = now
		}
		dl.mu.Unlock()
	})
	if err != nil {
		dl.mu.Lock()
		dl.Status = StatusFailed
		dl.Error = err.Error()
		dl.mu.Unlock()
		slog.Error("usenet download failed", "hash", dl.NZBHash, "err", err)
		return
	}

	// Post-processing: PAR2 repair + RAR extraction.
	dl.mu.Lock()
	dl.Status = StatusPostProcessing
	dl.mu.Unlock()

	slog.Info("usenet post-processing", "hash", dl.NZBHash)
	outputFiles, err := postproc.Process(dl.OutputDir, dl.NZBName)
	if err != nil {
		dl.mu.Lock()
		dl.Status = StatusFailed
		dl.Error = fmt.Sprintf("post-processing: %v", err)
		dl.mu.Unlock()
		slog.Error("usenet post-processing failed", "hash", dl.NZBHash, "err", err)
		return
	}

	// If post-processing produced files, use those. Otherwise use raw assembled files.
	if len(outputFiles) > 0 {
		dl.mu.Lock()
		dl.Files = outputFiles
		dl.Status = StatusComplete
		dl.mu.Unlock()
	} else {
		dl.mu.Lock()
		dl.Files = make([]postproc.OutputFile, len(results))
		for i, r := range results {
			dl.Files[i] = postproc.OutputFile{Name: r.Name, Path: r.Path, Size: r.Size}
		}
		dl.Status = StatusComplete
		dl.mu.Unlock()
	}

	slog.Info("usenet download complete", "hash", dl.NZBHash, "files", len(dl.Files))
}

func (m *Manager) getPool(ctx context.Context, userID string) (*nntp.Pool, error) {
	if v, ok := m.pools.Load(userID); ok {
		return v.(*nntp.Pool), nil
	}

	creds, err := m.credProvider.GetUsenetCredentials(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("get credentials: %w", err)
	}

	pool := nntp.NewPool(creds)
	actual, _ := m.pools.LoadOrStore(userID, pool)
	if actual.(*nntp.Pool) != pool {
		pool.Close()
	}
	return actual.(*nntp.Pool), nil
}

func (m *Manager) cleanupLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCleanup:
			return
		case <-ticker.C:
			m.pools.Range(func(key, value any) bool {
				pool := value.(*nntp.Pool)
				pool.CleanIdle()
				return true
			})
		}
	}
}

func (m *Manager) Close() {
	close(m.stopCleanup)
	m.pools.Range(func(key, value any) bool {
		value.(*nntp.Pool).Close()
		m.pools.Delete(key)
		return true
	})
}
