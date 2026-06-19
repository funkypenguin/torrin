package jobs

import (
	"testing"
	"time"
)

// TestReassignToSystem guards the delete-of-last-cached-copy path: deleting your
// only copy of cached content must move the job to the "system" user (kept in cache,
// gone from your account). The original code did `job.UserID = "system"; Update(job)`,
// but Update() doesn't write user_id, so it was a silent no-op and the job never left
// the user's account.
func TestReassignToSystem(t *testing.T) {
	store, err := NewStore(t.TempDir() + "/jobs.db")
	if err != nil {
		t.Fatal(err)
	}

	job := &Job{
		ID: "j1", UserID: "userA", InfoHash: "hashA", Status: StatusComplete,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := store.Create(job); err != nil {
		t.Fatal(err)
	}

	// Regression guard: Update() must NOT change user_id (this was the bug).
	job.UserID = "system"
	if err := store.Update(job); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.GetByID("j1"); got.UserID != "userA" {
		t.Fatalf("Update changed user_id to %q; it should never touch user_id", got.UserID)
	}

	// The fix: ReassignToSystem actually moves the owner.
	if err := store.ReassignToSystem("j1"); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetByID("j1")
	if err != nil {
		t.Fatal(err)
	}
	if got.UserID != "system" {
		t.Fatalf("ReassignToSystem failed: user_id = %q, want system", got.UserID)
	}
}
