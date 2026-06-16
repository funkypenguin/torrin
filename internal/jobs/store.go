package jobs

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type Store struct {
	db *sql.DB
}

func NewStore(dbPath string) (*Store, error) {
	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	if err := migrate(db); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}

	return &Store{db: db}, nil
}

func migrate(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS jobs (
			id          TEXT PRIMARY KEY,
			user_id     TEXT NOT NULL DEFAULT '',
			info_hash   TEXT NOT NULL,
			name        TEXT NOT NULL DEFAULT '',
			magnet      TEXT NOT NULL DEFAULT '',
			status      TEXT NOT NULL DEFAULT 'pending',
			error       TEXT NOT NULL DEFAULT '',
			files       TEXT NOT NULL DEFAULT '[]',
			selected    TEXT NOT NULL DEFAULT '[]',
			streams     TEXT NOT NULL DEFAULT '[]',
			created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX IF NOT EXISTS idx_jobs_infohash ON jobs(info_hash);
		CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
		CREATE INDEX IF NOT EXISTS idx_jobs_userid ON jobs(user_id);
	`)
	if err != nil {
		return err
	}

	db.Exec(`ALTER TABLE jobs ADD COLUMN last_accessed_at DATETIME`)
	db.Exec(`ALTER TABLE jobs ADD COLUMN access_count INTEGER NOT NULL DEFAULT 0`)
	db.Exec(`ALTER TABLE jobs ADD COLUMN file_size INTEGER NOT NULL DEFAULT 0`)
	db.Exec(`ALTER TABLE jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 0`)
	db.Exec(`ALTER TABLE jobs ADD COLUMN max_bytes INTEGER NOT NULL DEFAULT 0`)

	db.Exec(`CREATE INDEX IF NOT EXISTS idx_jobs_status_priority ON jobs(status, priority DESC, created_at ASC)`)

	db.Exec(`ALTER TABLE jobs ADD COLUMN source TEXT NOT NULL DEFAULT 'torrent'`)
	db.Exec(`ALTER TABLE jobs ADD COLUMN nzb_data BLOB`)
	db.Exec(`ALTER TABLE jobs ADD COLUMN imdb_id TEXT NOT NULL DEFAULT ''`)
	db.Exec(`CREATE INDEX IF NOT EXISTS idx_jobs_imdbid ON jobs(imdb_id)`)

	db.Exec(`CREATE TABLE IF NOT EXISTS views (
		info_hash TEXT NOT NULL,
		user_id   TEXT NOT NULL,
		viewed_on DATE NOT NULL DEFAULT (DATE('now')),
		PRIMARY KEY (info_hash, user_id, viewed_on)
	)`)

	db.Exec(`CREATE TABLE IF NOT EXISTS metrics_snapshots (
		date TEXT PRIMARY KEY,
		cached_count INTEGER DEFAULT 0,
		cached_size INTEGER DEFAULT 0,
		total_views INTEGER DEFAULT 0,
		total_users INTEGER DEFAULT 0,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	)`)

	return nil
}

func (s *Store) Create(job *Job) error {
	files, _ := json.Marshal(job.Files)
	selected, _ := json.Marshal(job.SelectedIdxs)
	streams, _ := json.Marshal(job.StreamURLs)

	source := job.Source
	if source == "" {
		source = "torrent"
	}

	_, err := s.db.Exec(`
		INSERT INTO jobs (id, user_id, info_hash, name, magnet, source, status, error, files, selected, streams, nzb_data, imdb_id, file_size, max_bytes, priority, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		job.ID, job.UserID, job.InfoHash, job.Name, job.Magnet, source, job.Status, job.Error,
		string(files), string(selected), string(streams), job.NZBData, job.IMDBID,
		job.FileSize, job.MaxBytes, job.Priority, job.CreatedAt, job.UpdatedAt,
	)
	return err
}

func (s *Store) Update(job *Job) error {
	files, _ := json.Marshal(job.Files)
	selected, _ := json.Marshal(job.SelectedIdxs)
	streams, _ := json.Marshal(job.StreamURLs)
	job.UpdatedAt = time.Now()

	_, err := s.db.Exec(`
		UPDATE jobs SET name=?, status=?, error=?, files=?, selected=?, streams=?, updated_at=?
		WHERE id=?`,
		job.Name, job.Status, job.Error,
		string(files), string(selected), string(streams),
		job.UpdatedAt, job.ID,
	)
	return err
}

func (s *Store) GetByID(id string) (*Job, error) {
	return s.scanOne(s.db.QueryRow(
		`SELECT id, user_id, info_hash, name, magnet, source, status, error, files, selected, streams, nzb_data, imdb_id, file_size, max_bytes, priority, created_at, updated_at FROM jobs WHERE id=?`, id))
}

func (s *Store) GetByInfoHash(hash string) (*Job, error) {
	return s.scanOne(s.db.QueryRow(
		`SELECT id, user_id, info_hash, name, magnet, source, status, error, files, selected, streams, nzb_data, imdb_id, file_size, max_bytes, priority, created_at, updated_at FROM jobs WHERE info_hash=? ORDER BY created_at DESC LIMIT 1`, hash))
}

func (s *Store) ListByInfoHash(hash string) ([]*Job, error) {
	return s.queryMany(
		`SELECT id, user_id, info_hash, name, magnet, source, status, error, files, selected, streams, nzb_data, imdb_id, file_size, max_bytes, priority, created_at, updated_at FROM jobs WHERE info_hash=?`, hash)
}

func (s *Store) List(limit int) ([]*Job, error) {
	if limit <= 0 {
		limit = 50
	}
	return s.queryMany(
		`SELECT id, user_id, info_hash, name, magnet, source, status, error, files, selected, streams, nzb_data, imdb_id, file_size, max_bytes, priority, created_at, updated_at FROM jobs ORDER BY created_at DESC LIMIT ?`, limit)
}

func (s *Store) ListByUser(userID string, limit int) ([]*Job, error) {
	if limit <= 0 {
		limit = 50
	}
	return s.queryMany(
		`SELECT id, user_id, info_hash, name, magnet, source, status, error, files, selected, streams, nzb_data, imdb_id, file_size, max_bytes, priority, created_at, updated_at FROM jobs WHERE user_id=? ORDER BY created_at DESC LIMIT ?`, userID, limit)
}

func (s *Store) ListByIMDB(imdbID string) ([]*Job, error) {
	return s.queryMany(
		`SELECT id, user_id, info_hash, name, magnet, source, status, error, files, selected, streams, nzb_data, imdb_id, file_size, max_bytes, priority, created_at, updated_at FROM jobs WHERE imdb_id=? AND status IN ('complete','cached') ORDER BY created_at DESC`, imdbID)
}

func (s *Store) ListByStatus(status Status) ([]*Job, error) {
	return s.queryMany(
		`SELECT id, user_id, info_hash, name, magnet, source, status, error, files, selected, streams, nzb_data, imdb_id, file_size, max_bytes, priority, created_at, updated_at FROM jobs WHERE status=? ORDER BY priority DESC, created_at ASC`, string(status))
}

func (s *Store) queryMany(query string, args ...any) ([]*Job, error) {
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*Job
	for rows.Next() {
		j, err := s.scanRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *Store) scanOne(row *sql.Row) (*Job, error) {
	j := &Job{}
	var filesJSON, selectedJSON, streamsJSON string
	var source sql.NullString
	err := row.Scan(
		&j.ID, &j.UserID, &j.InfoHash, &j.Name, &j.Magnet,
		&source, &j.Status, &j.Error,
		&filesJSON, &selectedJSON, &streamsJSON, &j.NZBData, &j.IMDBID,
		&j.FileSize, &j.MaxBytes, &j.Priority, &j.CreatedAt, &j.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	j.Source = source.String
	if j.Source == "" {
		j.Source = "torrent"
	}
	json.Unmarshal([]byte(filesJSON), &j.Files)
	json.Unmarshal([]byte(selectedJSON), &j.SelectedIdxs)
	json.Unmarshal([]byte(streamsJSON), &j.StreamURLs)
	return j, nil
}

func (s *Store) scanRow(rows *sql.Rows) (*Job, error) {
	j := &Job{}
	var filesJSON, selectedJSON, streamsJSON string
	var source sql.NullString
	err := rows.Scan(
		&j.ID, &j.UserID, &j.InfoHash, &j.Name, &j.Magnet,
		&source, &j.Status, &j.Error,
		&filesJSON, &selectedJSON, &streamsJSON, &j.NZBData, &j.IMDBID,
		&j.FileSize, &j.MaxBytes, &j.Priority, &j.CreatedAt, &j.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	j.Source = source.String
	if j.Source == "" {
		j.Source = "torrent"
	}
	json.Unmarshal([]byte(filesJSON), &j.Files)
	json.Unmarshal([]byte(selectedJSON), &j.SelectedIdxs)
	json.Unmarshal([]byte(streamsJSON), &j.StreamURLs)
	return j, nil
}

func (s *Store) RecordView(infoHash, userID string) (bool, error) {
	res, err := s.db.Exec(`INSERT OR IGNORE INTO views (info_hash, user_id) VALUES (?, ?)`, infoHash, userID)
	if err != nil {
		return false, err
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return false, nil
	}
	s.db.Exec(`UPDATE jobs SET last_accessed_at=CURRENT_TIMESTAMP, access_count=access_count+1, updated_at=CURRENT_TIMESTAMP
		WHERE info_hash=? AND (status='complete' OR status='cached')`, infoHash)
	return true, nil
}

type EvictionCandidate struct {
	ID              string
	InfoHash        string
	Name            string
	FileSize        int64
	AccessCount     int
	DaysSinceAccess int
}

func (s *Store) GetEvictionCandidates() ([]EvictionCandidate, error) {
	rows, err := s.db.Query(`
		SELECT MIN(id), info_hash, MAX(name), MAX(file_size), SUM(access_count),
			CAST(julianday('now') - julianday(MAX(COALESCE(last_accessed_at, created_at))) AS INTEGER) as days_inactive
		FROM jobs
		WHERE status IN ('complete', 'cached')
		GROUP BY info_hash
		ORDER BY
			CASE
				WHEN SUM(access_count) = 0 THEN 0
				WHEN SUM(access_count) < 10 THEN 1
				ELSE 2
			END ASC,
			days_inactive DESC,
			MAX(file_size) DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []EvictionCandidate
	for rows.Next() {
		var c EvictionCandidate
		if err := rows.Scan(&c.ID, &c.InfoHash, &c.Name, &c.FileSize, &c.AccessCount, &c.DaysSinceAccess); err != nil {
			continue
		}
		out = append(out, c)
	}
	return out, nil
}

func (s *Store) GetTotalCachedSize() (int64, error) {
	var total int64
	err := s.db.QueryRow(`SELECT COALESCE(SUM(max_size), 0) FROM (SELECT MAX(file_size) as max_size FROM jobs WHERE status IN ('complete', 'cached') GROUP BY info_hash)`).Scan(&total)
	return total, err
}

func (s *Store) SetFileSize(id string, size int64) error {
	_, err := s.db.Exec(`UPDATE jobs SET file_size=? WHERE id=?`, size, id)
	return err
}

func (s *Store) Delete(id string) error {
	_, err := s.db.Exec(`DELETE FROM jobs WHERE id=?`, id)
	return err
}

type UserStats struct {
	TotalDownloads int   `json:"total_downloads"`
	ActiveJobs     int   `json:"active_jobs"`
	CompletedJobs  int   `json:"completed_jobs"`
	FailedJobs     int   `json:"failed_jobs"`
	TotalBytes     int64 `json:"total_bytes"`
	TotalAccesses  int64 `json:"total_accesses"`
}

type WrappedContent struct {
	Name     string `json:"name"`
	InfoHash string `json:"info_hash"`
	ImdbID   string `json:"imdb_id,omitempty"`
	Views    int64  `json:"views"`
	Size     int64  `json:"size"`
}

type UserWrapped struct {
	TotalDownloads int              `json:"total_downloads"`
	TotalCached    int              `json:"total_cached"`
	TotalStreams   int64            `json:"total_streams"`
	TotalBytes     int64            `json:"total_bytes"`
	ActiveDays     int              `json:"active_days"`
	Streak         int              `json:"streak_days"`
	BiggestFile    int64            `json:"biggest_file"`
	TopContent     []WrappedContent `json:"top_content"`
	BySource       map[string]int   `json:"by_source"`
	MemberSince    string           `json:"member_since"`
}

type PlatformWrapped struct {
	TotalUsers     int              `json:"total_users"`
	TotalDownloads int              `json:"total_downloads"`
	TotalCached    int              `json:"total_cached"`
	TotalStreams   int64            `json:"total_streams"`
	TotalBytes     int64            `json:"total_bytes"`
	UniqueHashes   int              `json:"unique_hashes"`
	TopContent     []WrappedContent `json:"top_content"`
	BySource       map[string]int   `json:"by_source"`
	ActiveToday    int              `json:"active_today"`
}

func (s *Store) GetUserWrapped(userID string) (*UserWrapped, error) {
	w := &UserWrapped{BySource: make(map[string]int)}

	s.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE user_id=?`, userID).Scan(&w.TotalDownloads)
	s.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE user_id=? AND status IN ('complete','cached')`, userID).Scan(&w.TotalCached)
	s.db.QueryRow(`SELECT COALESCE(SUM(access_count), 0) FROM jobs WHERE user_id=?`, userID).Scan(&w.TotalStreams)
	s.db.QueryRow(`SELECT COALESCE(SUM(file_size), 0) FROM jobs WHERE user_id=? AND status IN ('complete','cached')`, userID).Scan(&w.TotalBytes)
	s.db.QueryRow(`SELECT COALESCE(MAX(file_size), 0) FROM jobs WHERE user_id=? AND status IN ('complete','cached')`, userID).Scan(&w.BiggestFile)
	s.db.QueryRow(`SELECT COUNT(DISTINCT date(created_at)) FROM jobs WHERE user_id=?`, userID).Scan(&w.ActiveDays)

	rows, _ := s.db.Query(`
		SELECT name, info_hash, imdb_id, access_count, file_size
		FROM jobs WHERE user_id=? AND status IN ('complete','cached') AND name != ''
		ORDER BY access_count DESC LIMIT 5`, userID)
	if rows != nil {
		defer rows.Close()
		for rows.Next() {
			var c WrappedContent
			rows.Scan(&c.Name, &c.InfoHash, &c.ImdbID, &c.Views, &c.Size)
			w.TopContent = append(w.TopContent, c)
		}
	}

	srcRows, _ := s.db.Query(`SELECT COALESCE(NULLIF(source,''), 'torrent'), COUNT(*) FROM jobs WHERE user_id=? GROUP BY source`, userID)
	if srcRows != nil {
		defer srcRows.Close()
		for srcRows.Next() {
			var src string
			var count int
			srcRows.Scan(&src, &count)
			w.BySource[src] = count
		}
	}

	streakRows, _ := s.db.Query(`SELECT DISTINCT date(created_at) as d FROM jobs WHERE user_id=? ORDER BY d DESC`, userID)
	if streakRows != nil {
		defer streakRows.Close()
		var dates []string
		for streakRows.Next() {
			var d string
			streakRows.Scan(&d)
			dates = append(dates, d)
		}
		w.Streak = calcStreak(dates)
	}

	return w, nil
}

func (s *Store) GetPlatformWrapped() (*PlatformWrapped, error) {
	p := &PlatformWrapped{BySource: make(map[string]int)}

	s.db.QueryRow(`SELECT COUNT(DISTINCT user_id) FROM jobs WHERE user_id != '' AND user_id != 'system'`).Scan(&p.TotalUsers)
	s.db.QueryRow(`SELECT COUNT(*) FROM jobs`).Scan(&p.TotalDownloads)
	s.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE status IN ('complete','cached')`).Scan(&p.TotalCached)
	s.db.QueryRow(`SELECT COALESCE(SUM(access_count), 0) FROM jobs`).Scan(&p.TotalStreams)
	s.db.QueryRow(`SELECT COALESCE(SUM(file_size), 0) FROM jobs WHERE status IN ('complete','cached')`).Scan(&p.TotalBytes)
	s.db.QueryRow(`SELECT COUNT(DISTINCT info_hash) FROM jobs WHERE status IN ('complete','cached')`).Scan(&p.UniqueHashes)
	s.db.QueryRow(`SELECT COUNT(DISTINCT user_id) FROM jobs WHERE date(created_at) = date('now')`).Scan(&p.ActiveToday)

	rows, _ := s.db.Query(`
		SELECT name, info_hash, imdb_id, SUM(access_count) as views, MAX(file_size) as size
		FROM jobs WHERE status IN ('complete','cached') AND name != ''
		GROUP BY info_hash ORDER BY views DESC LIMIT 10`)
	if rows != nil {
		defer rows.Close()
		for rows.Next() {
			var c WrappedContent
			rows.Scan(&c.Name, &c.InfoHash, &c.ImdbID, &c.Views, &c.Size)
			p.TopContent = append(p.TopContent, c)
		}
	}

	srcRows, _ := s.db.Query(`SELECT COALESCE(NULLIF(source,''), 'torrent'), COUNT(*) FROM jobs GROUP BY source`)
	if srcRows != nil {
		defer srcRows.Close()
		for srcRows.Next() {
			var src string
			var count int
			srcRows.Scan(&src, &count)
			p.BySource[src] = count
		}
	}

	return p, nil
}

func calcStreak(dates []string) int {
	if len(dates) == 0 {
		return 0
	}
	streak := 1
	for i := 1; i < len(dates); i++ {
		t1, e1 := time.Parse("2006-01-02", dates[i-1])
		t2, e2 := time.Parse("2006-01-02", dates[i])
		if e1 == nil && e2 == nil && t1.Sub(t2).Hours() == 24 {
			streak++
			continue
		}
		break
	}
	return streak
}

func (s *Store) GetUserStats(userID string) (*UserStats, error) {
	st := &UserStats{}
	s.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE user_id=?`, userID).Scan(&st.TotalDownloads)
	s.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE user_id=? AND status IN ('pending','queued','processing')`, userID).Scan(&st.ActiveJobs)
	s.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE user_id=? AND status IN ('complete','cached')`, userID).Scan(&st.CompletedJobs)
	s.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE user_id=? AND status='failed'`, userID).Scan(&st.FailedJobs)
	s.db.QueryRow(`SELECT COALESCE(SUM(file_size), 0) FROM jobs WHERE user_id=? AND status IN ('complete','cached')`, userID).Scan(&st.TotalBytes)
	s.db.QueryRow(`SELECT COALESCE(SUM(access_count), 0) FROM jobs WHERE user_id=?`, userID).Scan(&st.TotalAccesses)
	return st, nil
}

type HistoryEntry struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	InfoHash     string `json:"info_hash"`
	Status       Status `json:"status"`
	FileSize     int64  `json:"file_size"`
	AccessCount  int64  `json:"access_count"`
	CreatedAt    string `json:"created_at"`
	LastAccessed string `json:"last_accessed,omitempty"`
}

func (s *Store) GetUserHistory(userID string, limit int) ([]HistoryEntry, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.Query(`
		SELECT id, name, info_hash, status, file_size, access_count, created_at,
			COALESCE(last_accessed_at, '') as last_accessed
		FROM jobs WHERE user_id=? AND status IN ('complete','cached','evicted')
		ORDER BY created_at DESC LIMIT ?`, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []HistoryEntry
	for rows.Next() {
		var h HistoryEntry
		if err := rows.Scan(&h.ID, &h.Name, &h.InfoHash, &h.Status, &h.FileSize, &h.AccessCount, &h.CreatedAt, &h.LastAccessed); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	if out == nil {
		out = []HistoryEntry{}
	}
	return out, rows.Err()
}

type GlobalStats struct {
	TotalUsers       int   `json:"total_users"`
	TotalJobs        int   `json:"total_jobs"`
	TotalCachedGB    int64 `json:"total_cached_gb"`
	TotalAccessCount int64 `json:"total_access_count"`
}

type MetricsSnapshot struct {
	Date        string `json:"date"`
	CachedCount int    `json:"cached_count"`
	CachedSize  int64  `json:"cached_size"`
	TotalViews  int    `json:"total_views"`
	TotalUsers  int    `json:"total_users"`
}

func (s *Store) RecordDailySnapshot(totalUsers int) error {
	today := time.Now().Format("2006-01-02")
	_, err := s.db.Exec(`
		INSERT OR REPLACE INTO metrics_snapshots (date, cached_count, cached_size, total_views, total_users)
		VALUES (?,
			(SELECT COUNT(*) FROM jobs WHERE status IN ('complete','cached')),
			(SELECT COALESCE(SUM(max_size), 0) FROM (SELECT MAX(file_size) as max_size FROM jobs WHERE status IN ('complete','cached') GROUP BY info_hash)),
			(SELECT COALESCE(SUM(access_count), 0) FROM jobs),
			?
		)`, today, totalUsers)
	return err
}

func (s *Store) GetMetricsHistory(days int) ([]MetricsSnapshot, error) {
	rows, err := s.db.Query(`
		SELECT date, cached_count, cached_size, total_views, total_users
		FROM metrics_snapshots
		ORDER BY date DESC LIMIT ?`, days)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []MetricsSnapshot
	for rows.Next() {
		var m MetricsSnapshot
		if err := rows.Scan(&m.Date, &m.CachedCount, &m.CachedSize, &m.TotalViews, &m.TotalUsers); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	if out == nil {
		out = []MetricsSnapshot{}
	}
	return out, rows.Err()
}

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) Close() error {
	return s.db.Close()
}
