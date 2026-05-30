package nzb

import (
	"testing"
)

const sampleNZB = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <head>
    <meta type="name">Test Movie 2024</meta>
    <meta type="category">Movies</meta>
  </head>
  <file poster="poster@example.com" date="1700000000" subject="&quot;test.mkv&quot; (1/3)">
    <groups><group>alt.binaries.test</group></groups>
    <segments>
      <segment bytes="500000" number="1">seg1@example.com</segment>
      <segment bytes="500000" number="2">seg2@example.com</segment>
      <segment bytes="250000" number="3">seg3@example.com</segment>
    </segments>
  </file>
  <file poster="poster@example.com" date="1700000000" subject="&quot;test.nfo&quot; (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments>
      <segment bytes="1000" number="1">nfo1@example.com</segment>
    </segments>
  </file>
</nzb>`

func TestParseBytes(t *testing.T) {
	n, err := ParseBytes([]byte(sampleNZB))
	if err != nil {
		t.Fatalf("ParseBytes failed: %v", err)
	}

	if len(n.Files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(n.Files))
	}

	// First file: test.mkv with 3 segments.
	f := n.Files[0]
	if len(f.Segments) != 3 {
		t.Fatalf("expected 3 segments in first file, got %d", len(f.Segments))
	}
	if f.Segments[0].MessageID != "seg1@example.com" {
		t.Fatalf("unexpected messageID: %s", f.Segments[0].MessageID)
	}
	if f.Segments[0].Bytes != 500000 {
		t.Fatalf("expected 500000 bytes, got %d", f.Segments[0].Bytes)
	}
	if f.Segments[0].Number != 1 {
		t.Fatalf("expected segment number 1, got %d", f.Segments[0].Number)
	}

	// Second file: test.nfo with 1 segment.
	f2 := n.Files[1]
	if len(f2.Segments) != 1 {
		t.Fatalf("expected 1 segment in second file, got %d", len(f2.Segments))
	}

	// Groups.
	if len(f.Groups) != 1 || f.Groups[0] != "alt.binaries.test" {
		t.Fatalf("unexpected groups: %v", f.Groups)
	}
}

func TestParseBytes_Metadata(t *testing.T) {
	n, err := ParseBytes([]byte(sampleNZB))
	if err != nil {
		t.Fatal(err)
	}

	if n.Name() != "Test Movie 2024" {
		t.Fatalf("expected name 'Test Movie 2024', got %q", n.Name())
	}
	if n.Meta["category"] != "Movies" {
		t.Fatalf("expected category 'Movies', got %q", n.Meta["category"])
	}
}

func TestParseBytes_TotalSize(t *testing.T) {
	n, err := ParseBytes([]byte(sampleNZB))
	if err != nil {
		t.Fatal(err)
	}

	// 500000 + 500000 + 250000 + 1000 = 1251000
	expected := int64(1251000)
	if n.TotalSize() != expected {
		t.Fatalf("expected total size %d, got %d", expected, n.TotalSize())
	}
}

func TestParseBytes_RawPreserved(t *testing.T) {
	n, err := ParseBytes([]byte(sampleNZB))
	if err != nil {
		t.Fatal(err)
	}
	if len(n.Raw) == 0 {
		t.Fatal("Raw should be preserved")
	}
}

func TestParseBytes_Invalid(t *testing.T) {
	_, err := ParseBytes([]byte("not xml"))
	if err == nil {
		t.Fatal("expected error for invalid NZB")
	}
}

func TestParseBytes_Empty(t *testing.T) {
	_, err := ParseBytes([]byte(`<?xml version="1.0"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb"></nzb>`))
	if err != nil {
		t.Fatalf("empty NZB should parse: %v", err)
	}
}

func TestName_Fallbacks(t *testing.T) {
	// No metadata.
	n := &NZB{Meta: map[string]string{}}
	if n.Name() != "" {
		t.Fatalf("expected empty name, got %q", n.Name())
	}

	// Title fallback.
	n.Meta["title"] = "Fallback Title"
	if n.Name() != "Fallback Title" {
		t.Fatalf("expected 'Fallback Title', got %q", n.Name())
	}

	// Name takes priority.
	n.Meta["name"] = "Primary Name"
	if n.Name() != "Primary Name" {
		t.Fatalf("expected 'Primary Name', got %q", n.Name())
	}
}
