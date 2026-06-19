package jobs

import "testing"

func TestBYOS_QueueAndObjectRoundTrip(t *testing.T) {
	store, err := NewStore(t.TempDir() + "/jobs.db")
	if err != nil {
		t.Fatal(err)
	}

	// Enqueue is idempotent on job_id.
	if err := store.EnqueueBYOS("job1", "userA"); err != nil {
		t.Fatal(err)
	}
	if err := store.EnqueueBYOS("job1", "userA"); err != nil {
		t.Fatal(err)
	}
	if err := store.EnqueueBYOS("job2", "userB"); err != nil {
		t.Fatal(err)
	}

	q, err := store.ListBYOSQueue()
	if err != nil {
		t.Fatal(err)
	}
	if len(q) != 2 {
		t.Fatalf("expected 2 queued, got %d", len(q))
	}

	// Not in a user bucket yet.
	if _, ok := store.GetBYOSObject("job1"); ok {
		t.Fatal("job1 should not be marked yet")
	}

	// Mirror job1 -> mark object (with infohash + streams), drop from queue.
	streams := []Stream{{FileName: "movie.mkv", Size: 100, DirectURL: "hashA/file_0/movie.mkv"}}
	if err := store.MarkBYOSObject("job1", "userA", "hashA", "my-bucket", "My Movie", streams); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteBYOSQueue("job1"); err != nil {
		t.Fatal(err)
	}

	obj, ok := store.GetBYOSObject("job1")
	if !ok || obj.UserID != "userA" || obj.Bucket != "my-bucket" || obj.InfoHash != "hashA" {
		t.Fatalf("byos object wrong: %+v ok=%v", obj, ok)
	}

	// Retry lookup: the same user re-requesting hashA finds their bucket copy
	// (with the streams), so it can serve without re-downloading.
	owned, ok := store.GetBYOSObjectByUserHash("userA", "hashA")
	if !ok || owned.Bucket != "my-bucket" || len(owned.Streams) != 1 || owned.Streams[0].FileName != "movie.mkv" {
		t.Fatalf("user+hash lookup wrong: %+v ok=%v", owned, ok)
	}
	// A different user does not see it.
	if _, ok := store.GetBYOSObjectByUserHash("userB", "hashA"); ok {
		t.Fatal("userB should not own hashA")
	}

	q, _ = store.ListBYOSQueue()
	if len(q) != 1 || q[0].JobID != "job2" {
		t.Fatalf("expected only job2 queued, got %+v", q)
	}
}
