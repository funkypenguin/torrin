package nzb

import (
	"testing"
)

func TestHash_Deterministic(t *testing.T) {
	n, _ := ParseBytes([]byte(sampleNZB))

	h1 := Hash(n)
	h2 := Hash(n)
	if h1 != h2 {
		t.Fatalf("hash not deterministic: %s != %s", h1, h2)
	}
}

func TestHash_Length(t *testing.T) {
	n, _ := ParseBytes([]byte(sampleNZB))
	h := Hash(n)

	// Should be 40 hex chars (20 bytes).
	if len(h) != 40 {
		t.Fatalf("expected 40 char hash, got %d: %s", len(h), h)
	}

	// Should be valid hex.
	for _, c := range h {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Fatalf("invalid hex char %c in hash %s", c, h)
		}
	}
}

func TestHash_OrderIndependent(t *testing.T) {
	// Two NZBs with the same segments in different order should produce the same hash.
	nzb1 := &NZB{
		Files: []File{
			{Segments: []Segment{
				{MessageID: "aaa@test.com", Number: 1},
				{MessageID: "bbb@test.com", Number: 2},
			}},
		},
	}
	nzb2 := &NZB{
		Files: []File{
			{Segments: []Segment{
				{MessageID: "bbb@test.com", Number: 2},
				{MessageID: "aaa@test.com", Number: 1},
			}},
		},
	}

	if Hash(nzb1) != Hash(nzb2) {
		t.Fatal("hash should be order-independent")
	}
}

func TestHash_DifferentContent(t *testing.T) {
	nzb1 := &NZB{
		Files: []File{
			{Segments: []Segment{{MessageID: "aaa@test.com"}}},
		},
	}
	nzb2 := &NZB{
		Files: []File{
			{Segments: []Segment{{MessageID: "bbb@test.com"}}},
		},
	}

	if Hash(nzb1) == Hash(nzb2) {
		t.Fatal("different content should produce different hashes")
	}
}

func TestHash_MultiFile(t *testing.T) {
	// Segments across multiple files should all contribute to the hash.
	nzb1 := &NZB{
		Files: []File{
			{Segments: []Segment{{MessageID: "a@t"}}},
			{Segments: []Segment{{MessageID: "b@t"}}},
		},
	}
	nzb2 := &NZB{
		Files: []File{
			{Segments: []Segment{{MessageID: "a@t"}, {MessageID: "b@t"}}},
		},
	}

	// Same message IDs regardless of file grouping = same hash.
	if Hash(nzb1) != Hash(nzb2) {
		t.Fatal("hash should depend on message IDs, not file grouping")
	}
}
