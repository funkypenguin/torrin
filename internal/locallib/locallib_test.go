package locallib

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestRecordFromParsing(t *testing.T) {
	mov, _ := recordFrom("/lib/Apex.2026.2160p.NF.WEB-DL.DDP5.1.Atmos.DV.HDR.H.265-FLUX.mkv", "local", 18_000_000_000,
		"Apex.2026.2160p.NF.WEB-DL.DDP5.1.Atmos.DV.HDR.H.265-FLUX")
	if mov.Title != "Apex" || mov.Year != 2026 {
		t.Fatalf("movie title/year: got %q / %d", mov.Title, mov.Year)
	}
	if mov.Season != -1 || mov.Episode != 0 {
		t.Fatalf("movie should have season=-1 ep=0, got s=%d e=%d", mov.Season, mov.Episode)
	}
	if mov.Resolution != "2160p" {
		t.Fatalf("resolution: got %q", mov.Resolution)
	}

	ep, _ := recordFrom("/lib/The.Rookie.S08E01.720p.WEB.h264-WvF.mkv", "cold", 2_000_000_000,
		"The.Rookie.S08E01.720p.WEB.h264-WvF")
	if ep.Season != 8 || ep.Episode != 1 {
		t.Fatalf("episode S/E: got s=%d e=%d", ep.Season, ep.Episode)
	}
}

func TestLookupRoundtrip(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewStore(db)

	mov, norm := recordFrom("/lib/Apex.2026.2160p.NF.WEB-DL-FLUX.mkv", "local", 18_000_000_000,
		"Apex.2026.2160p.NF.WEB-DL-FLUX")
	s.upsert(mov, norm, 111)
	ep, norm2 := recordFrom("/lib/The.Rookie.S08E01.720p.WEB-WvF.mkv", "cold", 2_000_000_000,
		"The.Rookie.S08E01.720p.WEB-WvF")
	s.upsert(ep, norm2, 222)

	// Movie match by title + year.
	if got := s.Lookup([]string{"Apex"}, 2026, -1, 0); len(got) != 1 {
		t.Fatalf("movie exact: want 1, got %d", len(got))
	}
	// Year-agnostic match (year 0 = ignore).
	if got := s.Lookup([]string{"Apex"}, 0, -1, 0); len(got) != 1 {
		t.Fatalf("movie no-year: want 1, got %d", len(got))
	}
	// Wrong year should NOT match (disambiguates remakes).
	if got := s.Lookup([]string{"Apex"}, 1999, -1, 0); len(got) != 0 {
		t.Fatalf("movie wrong-year: want 0, got %d", len(got))
	}
	// Series match by title + season + episode.
	if got := s.Lookup([]string{"The Rookie"}, 0, 8, 1); len(got) != 1 {
		t.Fatalf("series exact: want 1, got %d", len(got))
	}
	// Wrong episode should NOT match.
	if got := s.Lookup([]string{"The Rookie"}, 0, 8, 2); len(got) != 0 {
		t.Fatalf("series wrong-ep: want 0, got %d", len(got))
	}
	// A movie request must not pull a series row.
	if got := s.Lookup([]string{"The Rookie"}, 0, -1, 0); len(got) != 0 {
		t.Fatalf("movie req hit series row: got %d", len(got))
	}

	// GetByID returns the on-disk path for streaming.
	rec := s.Lookup([]string{"Apex"}, 2026, -1, 0)[0]
	if got, ok := s.GetByID(rec.ID); !ok || got.Path != "/lib/Apex.2026.2160p.NF.WEB-DL-FLUX.mkv" {
		t.Fatalf("GetByID path: ok=%v path=%q", ok, got.Path)
	}
}
