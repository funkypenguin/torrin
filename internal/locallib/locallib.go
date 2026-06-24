// Package locallib turns on-disk media into an instant cache source.
package locallib

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	tnp "github.com/torrin-app/torrent-name-parser"
	"github.com/torrin-app/torrin/internal/jobs"
)

const minVideoBytes = 100 * 1000 * 1000

var videoExts = map[string]bool{
	".mkv": true, ".mp4": true, ".avi": true, ".m2ts": true,
	".ts": true, ".mov": true, ".wmv": true, ".webm": true, ".m4v": true,
}

type Record struct {
	ID         string `json:"id"`
	Path       string `json:"-"`
	FileSize   int64  `json:"size"`
	Title      string `json:"title"`
	Year       int    `json:"year"`
	Season     int    `json:"season"`
	Episode    int    `json:"episode"`
	Resolution string `json:"resolution"`
	Quality    string `json:"quality"`
	Languages  string `json:"languages"`
	Group      string `json:"group"`
	Root       string `json:"root"`
	Filename   string `json:"filename"`
}

type Store struct{ db *sql.DB }

func NewStore(db *sql.DB) *Store {
	db.Exec(`CREATE TABLE IF NOT EXISTS local_media (
		id          TEXT PRIMARY KEY,
		path        TEXT NOT NULL,
		file_size   INTEGER NOT NULL DEFAULT 0,
		title       TEXT NOT NULL DEFAULT '',
		title_norm  TEXT NOT NULL DEFAULT '',
		year        INTEGER NOT NULL DEFAULT 0,
		season      INTEGER NOT NULL DEFAULT -1,
		episode     INTEGER NOT NULL DEFAULT 0,
		resolution  TEXT NOT NULL DEFAULT '',
		quality     TEXT NOT NULL DEFAULT '',
		languages   TEXT NOT NULL DEFAULT '',
		grp         TEXT NOT NULL DEFAULT '',
		root        TEXT NOT NULL DEFAULT '',
		filename    TEXT NOT NULL DEFAULT '',
		mtime       INTEGER NOT NULL DEFAULT 0,
		indexed_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`)
	db.Exec(`CREATE INDEX IF NOT EXISTS idx_local_media_match ON local_media(title_norm, season, episode)`)
	return &Store{db: db}
}

func (s *Store) known() map[string]int64 {
	out := map[string]int64{}
	rows, err := s.db.Query(`SELECT path, mtime FROM local_media`)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var p string
		var m int64
		if rows.Scan(&p, &m) == nil {
			out[p] = m
		}
	}
	return out
}

func (s *Store) upsert(r Record, titleNorm string, mtime int64) {
	s.db.Exec(`INSERT OR REPLACE INTO local_media
		(id, path, file_size, title, title_norm, year, season, episode, resolution, quality, languages, grp, root, filename, mtime, indexed_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)`,
		r.ID, r.Path, r.FileSize, r.Title, titleNorm, r.Year, r.Season, r.Episode,
		r.Resolution, r.Quality, r.Languages, r.Group, r.Root, r.Filename, mtime)
}

func (s *Store) GetByID(id string) (Record, bool) {
	var r Record
	err := s.db.QueryRow(`SELECT id, path, file_size, title, year, season, episode, resolution, quality, languages, grp, root, filename
		FROM local_media WHERE id=?`, id).
		Scan(&r.ID, &r.Path, &r.FileSize, &r.Title, &r.Year, &r.Season, &r.Episode,
			&r.Resolution, &r.Quality, &r.Languages, &r.Group, &r.Root, &r.Filename)
	if err != nil {
		return Record{}, false
	}
	return r, true
}

func (s *Store) Lookup(titles []string, year, season, episode int) []Record {
	norms := map[string]bool{}
	var args []any
	for _, t := range titles {
		n := jobs.NormTitle(t)
		if n != "" && !norms[n] {
			norms[n] = true
			args = append(args, n)
		}
	}
	if len(args) == 0 {
		return nil
	}
	ph := strings.TrimSuffix(strings.Repeat("?,", len(args)), ",")

	var where string
	if season >= 0 && episode > 0 {
		where = `season=? AND episode=? AND title_norm IN (` + ph + `)`
		args = append([]any{season, episode}, args...)
	} else {
		where = `season=-1 AND title_norm IN (` + ph + `) AND (?=0 OR year=0 OR year=?)`
		args = append(args, year, year)
	}

	rows, err := s.db.Query(`SELECT id, path, file_size, title, year, season, episode, resolution, quality, languages, grp, root, filename
		FROM local_media WHERE `+where+` ORDER BY file_size DESC`, args...)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []Record
	for rows.Next() {
		var r Record
		if rows.Scan(&r.ID, &r.Path, &r.FileSize, &r.Title, &r.Year, &r.Season, &r.Episode,
			&r.Resolution, &r.Quality, &r.Languages, &r.Group, &r.Root, &r.Filename) == nil {
			out = append(out, r)
		}
	}
	return out
}

func fileID(path string) string {
	h := sha1.Sum([]byte(path))
	return hex.EncodeToString(h[:])
}

func cleanName(name string) string {
	name = strings.TrimSuffix(name, filepath.Ext(name))
	r := strings.NewReplacer(".", " ", "_", " ")
	return r.Replace(name)
}

func qualityOf(t tnp.Torrent) string {
	parts := []string{}
	if res := string(t.Resolution); res != "" {
		parts = append(parts, res)
	}
	if t.Source != "" {
		parts = append(parts, t.Source)
	}
	return strings.Join(parts, " ")
}

func recordFrom(path, root string, size int64, nameForParse string) (Record, string) {
	t, _ := tnp.ParseName(cleanName(nameForParse))
	season, episode := t.Season, t.Episode
	if season < 0 {
		season = -1
		episode = 0
	}
	r := Record{
		ID:         fileID(path),
		Path:       path,
		FileSize:   size,
		Title:      t.Title,
		Year:       t.Year,
		Season:     season,
		Episode:    episode,
		Resolution: string(t.Resolution),
		Quality:    qualityOf(t),
		Languages:  strings.Join(t.Languages, ","),
		Group:      t.Group,
		Root:       root,
		Filename:   filepath.Base(path),
	}
	return r, jobs.NormTitle(t.Title)
}

type vfile struct {
	path  string
	size  int64
	mtime int64
}

type topEntry struct {
	path  string
	isDir bool
}

func readTopLevel(base string) ([]topEntry, error) {
	ents, err := os.ReadDir(base)
	if err != nil {
		return nil, err
	}
	var out []topEntry
	for _, e := range ents {
		name := e.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		out = append(out, topEntry{path: filepath.Join(base, name), isDir: e.IsDir()})
	}
	return out, nil
}

func collectVideos(entry string) []vfile {
	var out []vfile
	filepath.WalkDir(entry, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if !videoExts[strings.ToLower(filepath.Ext(p))] {
			return nil
		}
		if strings.Contains(strings.ToLower(filepath.Base(p)), "sample") {
			return nil
		}
		info, err := d.Info()
		if err != nil || info.Size() < minVideoBytes {
			return nil
		}
		out = append(out, vfile{path: p, size: info.Size(), mtime: info.ModTime().Unix()})
		return nil
	})
	return out
}

func Scan(ctx context.Context, store *Store, roots map[string]string) {
	start := time.Now()
	known := store.known()
	seen := map[string]bool{}
	var added, updated int

	for label, base := range roots {
		entries, err := readTopLevel(base)
		if err != nil {
			slog.Warn("locallib: read root failed", "root", label, "path", base, "err", err)
			continue
		}
		for _, entry := range entries {
			select {
			case <-ctx.Done():
				return
			default:
			}
			vids := collectVideos(entry.path)
			if len(vids) == 0 {
				continue
			}
			multi := len(vids) > 1
			for _, v := range vids {
				seen[v.path] = true
				if mt, ok := known[v.path]; ok && mt == v.mtime {
					continue // unchanged
				}
				nameForParse := filepath.Base(v.path)
				if !multi && entry.isDir {
					nameForParse = filepath.Base(entry.path)
				}
				rec, norm := recordFrom(v.path, label, v.size, nameForParse)
				if rec.Title == "" {
					continue
				}
				if _, existed := known[v.path]; existed {
					updated++
				} else {
					added++
				}
				store.upsert(rec, norm, v.mtime)
			}
		}
	}

	var pruned int
	for p := range known {
		if !seen[p] {
			store.db.Exec(`DELETE FROM local_media WHERE path=?`, p)
			pruned++
		}
	}
	slog.Info("locallib: scan complete", "added", added, "updated", updated, "pruned", pruned, "took", time.Since(start).Round(time.Second))
}
