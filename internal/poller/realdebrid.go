package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/torrin-app/torrin/internal/jobs"
	"github.com/torrin-app/torrin/internal/realdebrid"
)

func (p *Poller) tryRealDebrid(ctx context.Context, job *jobs.Job) bool {
	if p.rd == nil || job.Magnet == "" {
		return false
	}

	if _, checking := p.uploading.Load("rd-check-" + job.InfoHash); checking {
		return false
	}
	p.uploading.Store("rd-check-"+job.InfoHash, true)
	defer p.uploading.Delete("rd-check-" + job.InfoHash)

	log := slog.With("job", job.ID, "hash", job.InfoHash)

	// Step 1: Add magnet to RD.
	added, err := p.rd.AddMagnet(ctx, job.Magnet)
	if err != nil {
		log.Warn("rd add magnet failed, falling through to qbit", "err", err)
		return false
	}
	rdID := added.ID

	// Step 2: Get torrent info to find video files, then select only those.
	info, err := p.rd.GetTorrent(ctx, rdID)
	if err != nil {
		log.Warn("rd get torrent failed", "err", err)
		p.rd.DeleteTorrent(ctx, rdID)
		return false
	}

	var videoFileIDs []string
	var totalVideoSize int64
	for _, f := range info.Files {
		if isVideoFile(f.Path) {
			videoFileIDs = append(videoFileIDs, fmt.Sprintf("%d", f.ID))
			totalVideoSize += f.Bytes
		}
	}

	// Size check against plan limit.
	if job.MaxBytes > 0 && totalVideoSize > job.MaxBytes {
		maxGB := job.MaxBytes / (1024 * 1024 * 1024)
		actualGB := totalVideoSize / (1024 * 1024 * 1024)
		log.Warn("rd torrent exceeds plan size limit", "size_gb", actualGB, "max_gb", maxGB)
		job.Status = jobs.StatusFailed
		job.Error = fmt.Sprintf("torrent size %dGB exceeds your plan limit of %dGB", actualGB, maxGB)
		p.store.Update(job)
		p.rd.DeleteTorrent(ctx, rdID)
		return true
	}
	if len(videoFileIDs) == 0 {
		// No video files found in file list, select all and hope for the best xD.
		videoFileIDs = []string{"all"}
	}

	selection := strings.Join(videoFileIDs, ",")
	if err := p.rd.SelectFiles(ctx, rdID, selection); err != nil {
		log.Warn("rd select files failed", "err", err)
		p.rd.DeleteTorrent(ctx, rdID)
		return false
	}

	// Step 3: Poll briefly to see if it's instantly available (cached).
	// Cached torrents resolve to "downloaded" within seconds.
	var torrent *realdebrid.Torrent
	cached := false
	for i := 0; i < 6; i++ {
		time.Sleep(5 * time.Second)

		t, err := p.rd.GetTorrent(ctx, rdID)
		if err != nil {
			log.Warn("rd poll failed", "err", err)
			p.rd.DeleteTorrent(ctx, rdID)
			return false
		}

		if t.Status == "downloaded" {
			torrent = t
			cached = true
			break
		}

		if t.Status == "error" || t.Status == "dead" || t.Status == "magnet_error" || t.Status == "virus" {
			log.Info("rd torrent failed, falling through to qbit", "status", t.Status)
			p.rd.DeleteTorrent(ctx, rdID)
			return false
		}

		// If it's actually downloading (not just resolving) then it's not cached.
		if t.Status == "downloading" && t.Progress < 100 && t.Progress > 0 {
			log.Info("not cached on rd, falling through to qbit", "progress", t.Progress)
			p.rd.DeleteTorrent(ctx, rdID)
			return false
		}
	}

	if !cached {
		log.Info("rd did not resolve in time, falling through to qbit")
		p.rd.DeleteTorrent(ctx, rdID)
		return false
	}

	// Re-fetch to ensure links are populated.
	torrent, err = p.rd.GetTorrent(ctx, rdID)
	if err != nil {
		log.Warn("rd re-fetch failed", "err", err)
		p.rd.DeleteTorrent(ctx, rdID)
		return false
	}

	log.Info("content cached on real-debrid, using RD path",
		"name", torrent.Filename, "links", len(torrent.Links), "files", len(torrent.Files))

	if len(torrent.Links) == 0 {
		log.Warn("rd cached but no links available, falling through to qbit")
		p.rd.DeleteTorrent(ctx, rdID)
		return false
	}

	// Start the RD download in a goroutine.
	if _, already := p.uploading.LoadOrStore(job.InfoHash, true); already {
		p.rd.DeleteTorrent(ctx, rdID)
		return true
	}

	job.Status = jobs.StatusProcessing
	job.Error = "downloading — 0%"
	if torrent.Filename != "" && job.Name == "" {
		job.Name = torrent.Filename
	}
	p.store.Update(job)

	go func(j *jobs.Job, t *realdebrid.Torrent, torrentID string) {
		defer p.uploading.Delete(j.InfoHash)
		p.downloadFromRD(ctx, j, t, torrentID)
	}(job, torrent, rdID)

	return true
}

// downloadFromRD handles downloading from RD after cache hit: unrestrict -> download -> upload to R2.
func (p *Poller) downloadFromRD(ctx context.Context, job *jobs.Job, torrent *realdebrid.Torrent, rdTorrentID string) {
	log := slog.With("job", job.ID, "hash", job.InfoHash, "rd_id", rdTorrentID)

	// Cleanup RD torrent when we're done.
	defer func() {
		if err := p.rd.DeleteTorrent(ctx, rdTorrentID); err != nil {
			log.Warn("rd cleanup failed", "err", err)
		}
	}()

	if len(torrent.Links) == 0 {
		log.Error("rd torrent has no links")
		job.Status = jobs.StatusFailed
		job.Error = "no download links available"
		p.store.Update(job)
		return
	}

	// Unrestrict each link and download.
	log.Info("unrestricting links", "count", len(torrent.Links))
	job.Error = "downloading"
	p.store.Update(job)

	tmpDir := filepath.Join(p.rdDownloadDir, job.InfoHash)
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		log.Error("mkdir failed", "err", err)
		job.Status = jobs.StatusFailed
		job.Error = fmt.Sprintf("mkdir: %v", err)
		p.store.Update(job)
		return
	}
	defer os.RemoveAll(tmpDir)

	type dlFile struct {
		Name string
		Path string
		Size int64
	}
	var downloadedFiles []dlFile

	for i, link := range torrent.Links {
		if ctx.Err() != nil {
			return
		}

		unrestricted, err := p.rd.Unrestrict(ctx, link)
		if err != nil {
			log.Warn("rd unrestrict failed", "link", link, "err", err)
			continue
		}

		log.Info("unrestricted", "filename", unrestricted.Filename, "size_mb", unrestricted.FileSize/(1024*1024), "url", unrestricted.Download[:min(80, len(unrestricted.Download))])

		if !isVideoFile(unrestricted.Filename) {
			log.Info("skipping non-video", "filename", unrestricted.Filename)
			continue
		}

		log.Info("downloading from rd", "file", unrestricted.Filename, "size_mb", unrestricted.FileSize/(1024*1024))

		pct := int(float64(i) / float64(len(torrent.Links)) * 100)
		job.Error = fmt.Sprintf("downloading — %d%%", pct)
		p.store.Update(job)

		localPath := filepath.Join(tmpDir, filepath.Base(unrestricted.Filename))
		if err := p.downloadRDFile(ctx, unrestricted.Download, localPath, job, unrestricted.FileSize); err != nil {
			log.Error("rd download failed", "file", unrestricted.Filename, "err", err)
			job.Status = jobs.StatusFailed
			job.Error = fmt.Sprintf("download failed: %v", err)
			p.store.Update(job)
			return
		}

		info, _ := os.Stat(localPath)
		var size int64
		if info != nil {
			size = info.Size()
		}

		downloadedFiles = append(downloadedFiles, dlFile{
			Name: unrestricted.Filename,
			Path: localPath,
			Size: size,
		})
	}

	if len(downloadedFiles) == 0 {
		log.Error("no video files downloaded from rd")
		job.Status = jobs.StatusFailed
		job.Error = "no video files found"
		p.store.Update(job)
		return
	}

	// Upload to R2.
	job.Error = "uploading to cache"
	p.store.Update(job)

	var streamURLs []jobs.Stream
	var uploadedSize int64

	for i, f := range downloadedFiles {
		safeBaseName := strings.ReplaceAll(f.Name, " ", "_")
		r2Key := fmt.Sprintf("%s/file_%d/%s", job.InfoHash, i, safeBaseName)

		log.Info("uploading to R2", "file", f.Name, "size_mb", f.Size/(1024*1024))

		file, err := os.Open(f.Path)
		if err != nil {
			log.Error("open file failed", "path", f.Path, "err", err)
			job.Status = jobs.StatusFailed
			job.Error = fmt.Sprintf("open: %v", err)
			p.store.Update(job)
			return
		}

		ct := contentTypeFor(filepath.Ext(f.Name))
		if err := p.r2.StreamUpload(ctx, r2Key, file, ct); err != nil {
			file.Close()
			log.Error("r2 upload failed", "key", r2Key, "err", err)
			job.Status = jobs.StatusFailed
			job.Error = fmt.Sprintf("upload: %v", err)
			p.store.Update(job)
			return
		}
		file.Close()
		uploadedSize += f.Size

		// Delete local file immediately after upload to save disk.
		os.Remove(f.Path)

		log.Info("uploaded", "key", r2Key, "size_mb", f.Size/(1024*1024))

		streamURLs = append(streamURLs, jobs.Stream{
			FileName:  f.Name,
			DirectURL: r2Key,
			SignedURL: p.r2.SignURL(r2Key, 24*time.Hour),
		})
	}

	// Create manifest and finalize.
	type manifestFile struct {
		FileName  string `json:"file_name"`
		DirectURL string `json:"direct_url"`
		FileSize  int64  `json:"file_size"`
	}
	var mFiles []manifestFile
	for i, s := range streamURLs {
		var sz int64
		if i < len(downloadedFiles) {
			sz = downloadedFiles[i].Size
		}
		mFiles = append(mFiles, manifestFile{FileName: s.FileName, DirectURL: s.DirectURL, FileSize: sz})
	}
	manifest := map[string]any{
		"info_hash": job.InfoHash, "name": job.Name,
		"files": mFiles, "created_at": time.Now(),
	}
	manifestJSON, _ := json.MarshalIndent(manifest, "", "  ")
	manifestKey := job.InfoHash + "/manifest.json"
	p.r2.UploadFile(ctx, manifestKey, strings.NewReader(string(manifestJSON)), "application/json")

	// Update all sibling jobs.
	siblings, _ := p.store.ListByInfoHash(job.InfoHash)
	for _, sib := range siblings {
		sib.StreamURLs = streamURLs
		sib.Name = job.Name
		sib.Status = jobs.StatusComplete
		sib.Error = ""
		p.store.Update(sib)
		p.store.SetFileSize(sib.ID, uploadedSize)
	}

	log.Info("job complete via real-debrid", "name", job.Name, "streams", len(streamURLs), "users", len(siblings))
}

// downloadRDFile downloads a file from an unrestricted RD URL to a local path,
// updating the job status with download progress.
func (p *Poller) downloadRDFile(ctx context.Context, downloadURL, localPath string, job *jobs.Job, totalSize int64) error {
	body, contentLength, err := p.rd.DownloadFile(ctx, downloadURL)
	if err != nil {
		return err
	}
	defer body.Close()

	if contentLength > 0 {
		totalSize = contentLength
	}

	f, err := os.Create(localPath)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := make([]byte, 256*1024) // 256KB buffer
	var written int64
	lastUpdate := time.Now()
	lastBytes := int64(0)

	for {
		n, readErr := body.Read(buf)
		if n > 0 {
			if _, wErr := f.Write(buf[:n]); wErr != nil {
				os.Remove(localPath)
				return wErr
			}
			written += int64(n)

			// Update progress every 2 seconds.
			if time.Since(lastUpdate) >= 2*time.Second {
				elapsed := time.Since(lastUpdate).Seconds()
				speed := float64(written-lastBytes) / elapsed
				speedMB := int(speed / (1024 * 1024))
				pct := 0
				if totalSize > 0 {
					pct = int(float64(written) / float64(totalSize) * 100)
				}
				msg := fmt.Sprintf("downloading — %d%% (%d MB/s)", pct, speedMB)
				if job.Error != msg {
					job.Error = msg
					p.store.Update(job)
				}
				lastUpdate = time.Now()
				lastBytes = written
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			os.Remove(localPath)
			return readErr
		}
	}
	return nil
}
