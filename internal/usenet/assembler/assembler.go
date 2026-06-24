package assembler

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/torrin-app/torrin/internal/usenet/decoder"
	"github.com/torrin-app/torrin/internal/usenet/nntp"
	"github.com/torrin-app/torrin/internal/usenet/nzb"
)

type FileResult struct {
	Name string
	Path string
	Size int64
}

type Assembler struct {
	pool *nntp.Pool
}

func New(pool *nntp.Pool) *Assembler {
	return &Assembler{pool: pool}
}

func (a *Assembler) DownloadAll(ctx context.Context, n *nzb.NZB, outputDir string, onProgress func(float64, int64)) ([]FileResult, error) {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}

	totalSegments := 0
	for _, f := range n.Files {
		totalSegments += len(f.Segments)
	}

	var completedSegments atomic.Int64
	var downloadedBytes atomic.Int64

	var results []FileResult
	for _, file := range n.Files {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// Skip PAR2 files.
		fname := extractFilename(file.Subject)
		if isSkippable(fname) || isSkippable(file.Subject) {
			slog.Info("skipping non-content file", "subject", file.Subject)
			continue
		}

		if name := candidateName(file); name != "" {
			outPath := filepath.Join(outputDir, name)
			if fi, err := os.Stat(outPath); err == nil && fi.Size() > 0 {
				slog.Info("resume: reusing completed file", "name", name, "size_mb", fi.Size()/1e6)
				done := completedSegments.Add(int64(len(file.Segments)))
				if onProgress != nil {
					onProgress(float64(done)/float64(totalSegments), downloadedBytes.Load())
				}
				results = append(results, FileResult{Name: name, Path: outPath, Size: fi.Size()})
				continue
			}
		}

		result, err := a.downloadFile(ctx, file, outputDir, func() {
			done := completedSegments.Add(1)
			progress := float64(done) / float64(totalSegments)
			if onProgress != nil {
				onProgress(progress, downloadedBytes.Load())
			}
		}, &downloadedBytes)
		if err != nil {
			// Log but continue for non-essential files (nfo, sfv, etc).
			if isOptional(fname) || isOptional(file.Subject) {
				slog.Warn("skipping failed optional file", "subject", file.Subject, "err", err)
				continue
			}
			return nil, fmt.Errorf("download %s: %w", file.Subject, err)
		}
		results = append(results, *result)
	}

	return results, nil
}

func (a *Assembler) downloadFile(ctx context.Context, file nzb.File, outputDir string, onSegment func(), downloadedBytes *atomic.Int64) (*FileResult, error) {
	// Sort segments by number.
	segments := make([]nzb.Segment, len(file.Segments))
	copy(segments, file.Segments)
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].Number < segments[j].Number
	})

	var totalSize int64

	// Determine filename: nzbparser parsed name > subject quotes > yEnc header > fallback.
	filename := file.Filename
	if filename == "" {
		filename = extractFilename(file.Subject)
	}

	// Use the first group from the NZB file for GROUP selection.
	group := ""
	if len(file.Groups) > 0 {
		group = file.Groups[0]
	}

	// If still no filename, peek the first segment's yEnc header.
	var firstSegData []byte
	if filename == "" {
		data, err := a.fetchSegment(segments[0].MessageID, group)
		if err != nil {
			return nil, fmt.Errorf("peek segment: %w", err)
		}
		firstSegData = data
		// Try raw body for yEnc name (fetchSegment already decoded, so re-fetch raw).
		conn, cerr := a.pool.Get()
		if cerr == nil {
			if group != "" {
				conn.Group(group)
			}
			raw, berr := conn.Body(segments[0].MessageID)
			a.pool.Put(conn)
			if berr == nil {
				if yr, derr := decoder.Decode(raw); derr == nil && yr.Filename != "" {
					filename = yr.Filename
				}
			}
		}
		if filename == "" {
			filename = fmt.Sprintf("file_%d", segments[0].Number)
		}
	}
	filename = filepath.Base(filename)
	outPath := filepath.Join(outputDir, filename)
	if !strings.HasPrefix(outPath, filepath.Clean(outputDir)+string(os.PathSeparator)) {
		return nil, fmt.Errorf("unsafe filename rejected: %s", filename)
	}

	partPath := outPath + ".part"
	f, err := os.Create(partPath)
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", partPath, err)
	}
	defer f.Close()

	// If we already fetched the first segment, write it now.
	if firstSegData != nil {
		n, werr := f.Write(firstSegData)
		if werr != nil {
			return nil, fmt.Errorf("write: %w", werr)
		}
		totalSize += int64(n)
		downloadedBytes.Add(int64(n))
		if onSegment != nil {
			onSegment()
		}
		segments = segments[1:]
	}

	type segResult struct {
		index int
		data  []byte
		err   error
	}

	// Fixed worker pool.
	numWorkers := a.pool.MaxConns()
	if numWorkers > len(segments) {
		numWorkers = len(segments)
	}
	resultCh := make(chan segResult, numWorkers*2)

	// Workers pull from a job channel.
	go func() {
		defer close(resultCh)
		var wg sync.WaitGroup
		jobCh := make(chan int, numWorkers)

		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for idx := range jobCh {
					if ctx.Err() != nil {
						return
					}
					seg := segments[idx]
					data, err := a.fetchSegment(seg.MessageID, group)
					resultCh <- segResult{index: idx, data: data, err: err}
				}
			}()
		}

		for i := range segments {
			if ctx.Err() != nil {
				break
			}
			jobCh <- i
		}
		close(jobCh)
		wg.Wait()
	}()

	// Writer: consume results, write in order, free memory immediately.
	nextToWrite := 0
	pending := make(map[int][]byte)

	for r := range resultCh {
		if r.err != nil {
			return nil, fmt.Errorf("segment %d (%s): %w",
				segments[r.index].Number, segments[r.index].MessageID, r.err)
		}

		downloadedBytes.Add(int64(len(r.data)))
		if onSegment != nil {
			onSegment()
		}

		pending[r.index] = r.data

		// Write all consecutive segments starting from nextToWrite.
		for {
			data, ok := pending[nextToWrite]
			if !ok {
				break
			}
			n, err := f.Write(data)
			if err != nil {
				return nil, fmt.Errorf("write: %w", err)
			}
			totalSize += int64(n)
			delete(pending, nextToWrite)
			nextToWrite++
			// Flush to disk periodically to prevent dirty page buildup.
			if nextToWrite%50 == 0 {
				f.Sync()
			}
		}
	}

	f.Sync()
	f.Close()
	if err := os.Rename(partPath, outPath); err != nil {
		return nil, fmt.Errorf("finalize %s: %w", filename, err)
	}

	slog.Info("assembled file", "name", filename, "size_mb", totalSize/1e6, "segments", len(segments))

	return &FileResult{
		Name: filename,
		Path: outPath,
		Size: totalSize,
	}, nil
}

func (a *Assembler) fetchSegment(messageID, group string) ([]byte, error) {
	// Retry up to 3 times on failure, with fresh connections on retry.
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		conn, err := a.pool.Get()
		if err != nil {
			lastErr = err
			continue
		}

		// Select group before BODY.
		if group != "" {
			if err := conn.Group(group); err != nil {
				a.pool.Discard(conn)
				lastErr = err
				continue
			}
		}

		body, err := conn.Body(messageID)
		if err != nil {
			slog.Warn("segment fetch failed", "id", messageID, "attempt", attempt+1, "err", err)
			a.pool.Discard(conn)
			lastErr = err
			continue
		}
		a.pool.Put(conn)

		// Decode yEnc.
		result, err := decoder.Decode(body)
		if err != nil {
			// Not yEnc encoded, return raw.
			return body, nil
		}
		return result.Data, nil
	}
	return nil, fmt.Errorf("after 3 attempts: %w", lastErr)
}

func candidateName(file nzb.File) string {
	name := file.Filename
	if name == "" {
		name = extractFilename(file.Subject)
	}
	if name == "" {
		return ""
	}
	return filepath.Base(name)
}

func isSkippable(name string) bool {
	lower := strings.ToLower(name)
	return strings.HasSuffix(lower, ".par2") || strings.Contains(lower, ".par2") ||
		strings.HasSuffix(lower, ".nzb")
}

func isOptional(name string) bool {
	lower := strings.ToLower(name)
	return strings.HasSuffix(lower, ".nfo") || strings.HasSuffix(lower, ".sfv") ||
		strings.HasSuffix(lower, ".txt") || strings.HasSuffix(lower, ".jpg") ||
		strings.HasSuffix(lower, ".png")
}

func extractFilename(subject string) string {
	start := -1
	for i, c := range subject {
		if c == '"' {
			if start < 0 {
				start = i + 1
			} else {
				return subject[start:i]
			}
		}
	}
	return ""
}
