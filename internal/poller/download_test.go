package poller

import (
	"context"
	"io"
	"os"
	"testing"
)

// chunkReader yields data, optionally erroring after failAfter bytes to simulate a
// dropped connection mid-stream.
type chunkReader struct {
	data      []byte
	pos       int
	failAfter int // -1 = never fail
	err       error
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if r.failAfter >= 0 && r.pos >= r.failAfter {
		return 0, r.err
	}
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	end := len(r.data)
	if r.failAfter >= 0 && r.failAfter < end {
		end = r.failAfter
	}
	n := copy(p, r.data[r.pos:end])
	r.pos += n
	return n, nil
}

func (r *chunkReader) Close() error { return nil }

func makeData(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

func runResilient(t *testing.T, open rangeOpener) ([]byte, error) {
	t.Helper()
	orig := downloadBackoff
	downloadBackoff = func(context.Context, int) {} // no real sleeps in tests
	defer func() { downloadBackoff = orig }()
	f, err := os.CreateTemp(t.TempDir(), "dl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := downloadResilient(context.Background(), open, f, nil); err != nil {
		return nil, err
	}
	f.Seek(0, io.SeekStart)
	out, _ := io.ReadAll(f)
	return out, nil
}

// Connection drops at 600/1000 bytes, then resumes via a 206 with the remainder.
func TestResilient_ResumeOnDrop(t *testing.T) {
	full := makeData(1000)
	calls := 0
	open := func(offset int64) (io.ReadCloser, int64, bool, error) {
		calls++
		switch calls {
		case 1:
			if offset != 0 {
				t.Fatalf("first call offset = %d, want 0", offset)
			}
			return &chunkReader{data: full, failAfter: 600, err: io.ErrUnexpectedEOF}, 1000, true, nil
		case 2:
			if offset != 600 {
				t.Fatalf("resume offset = %d, want 600", offset)
			}
			// 206 Partial Content: remainder only, total still 1000.
			return &chunkReader{data: full[600:], failAfter: -1}, 1000, false, nil
		}
		t.Fatalf("unexpected extra call %d", calls)
		return nil, 0, false, nil
	}
	out, err := runResilient(t, open)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if len(out) != 1000 {
		t.Fatalf("len = %d, want 1000", len(out))
	}
	for i := range out {
		if out[i] != full[i] {
			t.Fatalf("byte %d = %d, want %d", i, out[i], full[i])
		}
	}
}

// Server ignores the Range header and replies 200 with the whole file again — the
// downloader must restart from 0, not corrupt the file by appending.
func TestResilient_ServerIgnoresRange(t *testing.T) {
	full := makeData(800)
	calls := 0
	open := func(offset int64) (io.ReadCloser, int64, bool, error) {
		calls++
		if calls == 1 {
			return &chunkReader{data: full, failAfter: 500, err: io.ErrUnexpectedEOF}, 800, true, nil
		}
		// full=true on resume: whole file from byte 0 again.
		return &chunkReader{data: full, failAfter: -1}, 800, true, nil
	}
	out, err := runResilient(t, open)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if len(out) != 800 {
		t.Fatalf("len = %d, want 800 (restart, not append)", len(out))
	}
	for i := range out {
		if out[i] != full[i] {
			t.Fatalf("byte %d corrupted", i)
		}
	}
}

// A clean EOF that is short of the declared total counts as truncation and resumes.
func TestResilient_TruncationDetected(t *testing.T) {
	full := makeData(1000)
	calls := 0
	open := func(offset int64) (io.ReadCloser, int64, bool, error) {
		calls++
		if calls == 1 {
			// clean EOF at 400 but total is 1000 -> truncated.
			return &chunkReader{data: full[:400], failAfter: -1}, 1000, true, nil
		}
		if offset != 400 {
			t.Fatalf("resume offset = %d, want 400", offset)
		}
		return &chunkReader{data: full[400:], failAfter: -1}, 1000, false, nil
	}
	out, err := runResilient(t, open)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if len(out) != 1000 {
		t.Fatalf("len = %d, want 1000", len(out))
	}
}

// Give up after maxDownloadAttempts if it never recovers.
func TestResilient_ExhaustsAttempts(t *testing.T) {
	full := makeData(1000)
	open := func(offset int64) (io.ReadCloser, int64, bool, error) {
		return &chunkReader{data: full, failAfter: 100, err: io.ErrUnexpectedEOF}, 1000, true, nil
	}
	if _, err := runResilient(t, open); err == nil {
		t.Fatal("expected failure after exhausting attempts")
	}
}
