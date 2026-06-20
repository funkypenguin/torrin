package jobs

import (
	"database/sql"
	"encoding/json"
)

// BYOSQueueItem is a completed job whose owner uses their own storage; the byos
// container copies it from R2 into the user's bucket.
type BYOSQueueItem struct {
	JobID  string
	UserID string
}

// EnqueueBYOS marks a completed job for mirroring into the user's own bucket.
func (s *Store) EnqueueBYOS(jobID, userID string) error {
	_, err := s.db.Exec(`
		INSERT INTO byos_queue (job_id, user_id) VALUES (?, ?)
		ON CONFLICT(job_id) DO NOTHING`,
		jobID, userID,
	)
	return err
}

// ListBYOSQueue returns pending mirror jobs (for the byos container).
func (s *Store) ListBYOSQueue() ([]BYOSQueueItem, error) {
	rows, err := s.db.Query(`SELECT job_id, user_id FROM byos_queue ORDER BY created_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BYOSQueueItem
	for rows.Next() {
		var it BYOSQueueItem
		if err := rows.Scan(&it.JobID, &it.UserID); err != nil {
			continue
		}
		out = append(out, it)
	}
	return out, nil
}

func (s *Store) DeleteBYOSQueue(jobID string) error {
	_, err := s.db.Exec(`DELETE FROM byos_queue WHERE job_id=?`, jobID)
	return err
}

func (s *Store) IncrementBYOSAttempt(jobID string) int {
	s.db.Exec(`UPDATE byos_queue SET attempts = attempts + 1 WHERE job_id=?`, jobID)
	var n int
	s.db.QueryRow(`SELECT attempts FROM byos_queue WHERE job_id=?`, jobID).Scan(&n)
	return n
}

// BYOSObject records that a finished job lives in a user's own bucket. The
// info_hash + streams let a later re-request for the same content (after the
// shared-R2 copy has evicted) serve straight from the user's bucket instead of
// re-downloading.
type BYOSObject struct {
	UserID   string
	Bucket   string
	InfoHash string
	Name     string
	Streams  []Stream
}

func (s *Store) MarkBYOSObject(jobID, userID, infoHash, bucket, name string, streams []Stream) error {
	data, _ := json.Marshal(streams)
	_, err := s.db.Exec(`
		INSERT INTO byos_objects (job_id, user_id, bucket, info_hash, name, streams_json)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(job_id) DO UPDATE SET user_id=?, bucket=?, info_hash=?, name=?, streams_json=?`,
		jobID, userID, bucket, infoHash, name, string(data),
		userID, bucket, infoHash, name, string(data),
	)
	return err
}

// GetBYOSObject reports whether a job is stored in a user bucket (vs shared R2).
func (s *Store) GetBYOSObject(jobID string) (*BYOSObject, bool) {
	return s.scanBYOSObject(s.db.QueryRow(
		`SELECT user_id, bucket, info_hash, name, streams_json FROM byos_objects WHERE job_id=?`, jobID))
}

// GetBYOSObjectByUserHash finds content a user already owns in their bucket, so
// a re-request can skip the download.
func (s *Store) GetBYOSObjectByUserHash(userID, infoHash string) (*BYOSObject, bool) {
	return s.scanBYOSObject(s.db.QueryRow(
		`SELECT user_id, bucket, info_hash, name, streams_json FROM byos_objects
		 WHERE user_id=? AND info_hash=? ORDER BY created_at DESC LIMIT 1`, userID, infoHash))
}

func (s *Store) scanBYOSObject(row *sql.Row) (*BYOSObject, bool) {
	o := &BYOSObject{}
	var streamsJSON string
	if err := row.Scan(&o.UserID, &o.Bucket, &o.InfoHash, &o.Name, &streamsJSON); err != nil {
		return nil, false
	}
	json.Unmarshal([]byte(streamsJSON), &o.Streams)
	return o, true
}
