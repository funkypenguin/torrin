package postproc

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCollectFiles_SkipsArchives(t *testing.T) {
	dir := t.TempDir()

	// Create various files.
	files := map[string]string{
		"movie.mkv":           "video data",
		"movie.nfo":           "info",
		"movie.par2":          "parity",
		"movie.vol00+01.par2": "parity vol",
		"movie.rar":           "archive",
		"movie.r00":           "archive part",
		"movie.r01":           "archive part",
		"subs.srt":            "subtitles",
	}
	for name, content := range files {
		os.WriteFile(filepath.Join(dir, name), []byte(content), 0644)
	}

	results, err := collectFiles(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Should only include .mkv, .nfo, .srt (not .par2, .rar, .rNN).
	names := make(map[string]bool)
	for _, f := range results {
		names[f.Name] = true
	}

	if !names["movie.mkv"] {
		t.Fatal("expected movie.mkv")
	}
	if !names["movie.nfo"] {
		t.Fatal("expected movie.nfo")
	}
	if !names["subs.srt"] {
		t.Fatal("expected subs.srt")
	}
	if names["movie.par2"] {
		t.Fatal("should skip .par2")
	}
	if names["movie.rar"] {
		t.Fatal("should skip .rar")
	}
	if names["movie.r00"] {
		t.Fatal("should skip .r00")
	}
}

func TestIsRarPart(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{"file.r00", true},
		{"file.r01", true},
		{"file.r99", true},
		{"file.rar", false},
		{"file.txt", false},
		{"f.r0", false},
		{".r00", true},
	}
	for _, tt := range tests {
		got := isRarPart(tt.name)
		if got != tt.want {
			t.Errorf("isRarPart(%q) = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestFindFirstRar(t *testing.T) {
	dir := t.TempDir()

	// No rar files.
	if findFirstRar(dir) != "" {
		t.Fatal("expected empty for no rar files")
	}

	// Single rar.
	os.WriteFile(filepath.Join(dir, "movie.rar"), []byte("x"), 0644)
	if filepath.Base(findFirstRar(dir)) != "movie.rar" {
		t.Fatal("expected movie.rar")
	}

	// Part01 takes priority.
	os.WriteFile(filepath.Join(dir, "movie.part01.rar"), []byte("x"), 0644)
	if filepath.Base(findFirstRar(dir)) != "movie.part01.rar" {
		t.Fatal("expected movie.part01.rar to take priority")
	}
}

func TestFindFile(t *testing.T) {
	dir := t.TempDir()

	os.WriteFile(filepath.Join(dir, "file.par2"), []byte("x"), 0644)
	os.WriteFile(filepath.Join(dir, "file.vol00+01.par2"), []byte("x"), 0644)

	// Find .par2 but exclude "vol" files.
	result := findFile(dir, ".par2", "vol")
	if result == "" {
		t.Fatal("expected to find file.par2")
	}
	if filepath.Base(result) != "file.par2" {
		t.Fatalf("expected file.par2, got %s", filepath.Base(result))
	}
}

func TestCleanArchives(t *testing.T) {
	dir := t.TempDir()

	keep := []string{"movie.mkv", "subs.srt"}
	remove := []string{"movie.rar", "movie.r00", "movie.r01", "movie.par2", "movie.nzb"}

	for _, f := range append(keep, remove...) {
		os.WriteFile(filepath.Join(dir, f), []byte("x"), 0644)
	}

	cleanArchives(dir)

	entries, _ := os.ReadDir(dir)
	names := make(map[string]bool)
	for _, e := range entries {
		names[e.Name()] = true
	}

	for _, f := range keep {
		if !names[f] {
			t.Fatalf("%s should have been kept", f)
		}
	}
	for _, f := range remove {
		if names[f] {
			t.Fatalf("%s should have been removed", f)
		}
	}
}

func TestProcess_NoArchives(t *testing.T) {
	dir := t.TempDir()

	os.WriteFile(filepath.Join(dir, "movie.mkv"), []byte("video content"), 0644)
	os.WriteFile(filepath.Join(dir, "info.nfo"), []byte("nfo"), 0644)

	results, err := Process(dir)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 files, got %d", len(results))
	}

	hasVideo := false
	for _, f := range results {
		if f.Name == "movie.mkv" {
			hasVideo = true
			if f.Size != 13 { // len("video content")
				t.Fatalf("expected size 13, got %d", f.Size)
			}
		}
	}
	if !hasVideo {
		t.Fatal("expected movie.mkv in results")
	}
}
