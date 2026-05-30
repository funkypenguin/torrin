package decoder

import (
	"bytes"
	"testing"
)

func TestDecode_SimpleMessage(t *testing.T) {
	raw := "=ybegin part=1 line=128 size=5 name=test.txt\r\n" +
		"=ypart begin=1 end=5\r\n" +
		yencEncode([]byte("Hello")) + "\r\n" +
		"=yend size=5\r\n"

	result, err := Decode([]byte(raw))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if string(result.Data) != "Hello" {
		t.Fatalf("expected 'Hello', got %q", result.Data)
	}
	if result.Filename != "test.txt" {
		t.Fatalf("expected filename 'test.txt', got %q", result.Filename)
	}
	if result.Part != 1 {
		t.Fatalf("expected part 1, got %d", result.Part)
	}
	if result.Size != 5 {
		t.Fatalf("expected size 5, got %d", result.Size)
	}
}

func TestDecode_BinaryData(t *testing.T) {
	// Test with binary data (all byte values 0-255).
	original := make([]byte, 256)
	for i := range original {
		original[i] = byte(i)
	}

	encoded := yencEncode(original)
	raw := "=ybegin line=128 size=256 name=binary.bin\r\n" +
		encoded + "\r\n" +
		"=yend size=256\r\n"

	result, err := Decode([]byte(raw))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if !bytes.Equal(result.Data, original) {
		t.Fatalf("binary roundtrip failed: got %d bytes, expected %d", len(result.Data), len(original))
	}
}

func TestDecode_MultipartHeaders(t *testing.T) {
	raw := "=ybegin part=3 total=10 line=128 size=100 name=movie.mkv\r\n" +
		"=ypart begin=201 end=300\r\n" +
		yencEncode([]byte("test")) + "\r\n" +
		"=yend size=4\r\n"

	result, err := Decode([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if result.Part != 3 {
		t.Fatalf("expected part 3, got %d", result.Part)
	}
	if result.Total != 10 {
		t.Fatalf("expected total 10, got %d", result.Total)
	}
	if result.Begin != 201 {
		t.Fatalf("expected begin 201, got %d", result.Begin)
	}
	if result.End != 300 {
		t.Fatalf("expected end 300, got %d", result.End)
	}
	if result.Filename != "movie.mkv" {
		t.Fatalf("expected filename 'movie.mkv', got %q", result.Filename)
	}
}

func TestDecode_NoYencData(t *testing.T) {
	_, err := Decode([]byte("just plain text\r\n"))
	if err == nil {
		t.Fatal("expected error for non-yenc data")
	}
}

func TestDecode_EmptyBody(t *testing.T) {
	raw := "=ybegin line=128 size=0 name=empty.txt\r\n" +
		"=yend size=0\r\n"

	result, err := Decode([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Data) != 0 {
		t.Fatalf("expected empty data, got %d bytes", len(result.Data))
	}
}

func TestDecode_LFOnly(t *testing.T) {
	raw := "=ybegin line=128 size=3 name=lf.txt\n" +
		yencEncode([]byte("abc")) + "\n" +
		"=yend size=3\n"

	result, err := Decode([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if string(result.Data) != "abc" {
		t.Fatalf("expected 'abc', got %q", result.Data)
	}
}

func TestExtractValue(t *testing.T) {
	tests := []struct {
		line, key, want string
	}{
		{"=ybegin size=100 name=test.txt", "size", "100"},
		{"=ybegin size=100 name=test.txt", "name", "test.txt"},
		{"=ybegin size=100 name=file with spaces.mkv", "name", "file with spaces.mkv"},
		{"=ypart begin=1 end=500", "begin", "1"},
		{"=ypart begin=1 end=500", "end", "500"},
		{"=ybegin size=100", "missing", ""},
	}

	for _, tt := range tests {
		got := extractValue(tt.line, tt.key)
		if got != tt.want {
			t.Errorf("extractValue(%q, %q) = %q, want %q", tt.line, tt.key, got, tt.want)
		}
	}
}

// yencEncode encodes bytes to yEnc format for testing.
func yencEncode(data []byte) string {
	var buf bytes.Buffer
	for _, b := range data {
		encoded := byte((int(b) + 42) % 256)
		if encoded == 0 || encoded == '\n' || encoded == '\r' || encoded == '=' || encoded == '.' {
			buf.WriteByte('=')
			encoded = byte((int(encoded) + 64) % 256)
		}
		buf.WriteByte(encoded)
	}
	return buf.String()
}
