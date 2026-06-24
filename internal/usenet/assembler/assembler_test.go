package assembler_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"os"
	"strings"
	"testing"

	gonntp "github.com/dustin/go-nntp"
	nntpserver "github.com/dustin/go-nntp/server"
	"github.com/torrin-app/torrin/internal/usenet/assembler"
	"github.com/torrin-app/torrin/internal/usenet/nntp"
	"github.com/torrin-app/torrin/internal/usenet/nzb"
)

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

func makeYencArticle(data []byte, part, total int, filename string) []byte {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "=ybegin part=%d total=%d line=128 size=%d name=%s\r\n", part, total, len(data), filename)
	fmt.Fprintf(&buf, "=ypart begin=%d end=%d\r\n", 1, len(data))
	buf.WriteString(yencEncode(data))
	buf.WriteString("\r\n")
	fmt.Fprintf(&buf, "=yend size=%d\r\n", len(data))
	return buf.Bytes()
}

type asmBackend struct {
	articles map[string][]byte
}

func newAsmBackend() *asmBackend {
	return &asmBackend{articles: make(map[string][]byte)}
}

func (b *asmBackend) addArticle(id string, body []byte) {
	b.articles[id] = body
}

func (b *asmBackend) ListGroups(max int) ([]*gonntp.Group, error) {
	return []*gonntp.Group{{Name: "alt.binaries.test", High: 1, Low: 1, Count: 1}}, nil
}
func (b *asmBackend) GetGroup(name string) (*gonntp.Group, error) {
	return &gonntp.Group{Name: name, High: 1, Low: 1, Count: 1}, nil
}
func (b *asmBackend) GetArticle(group *gonntp.Group, id string) (*gonntp.Article, error) {
	id = strings.TrimPrefix(id, "<")
	id = strings.TrimSuffix(id, ">")
	body, ok := b.articles[id]
	if !ok {
		return nil, fmt.Errorf("430 not found: %s", id)
	}
	h := textproto.MIMEHeader{}
	h.Set("Message-Id", "<"+id+">")
	return &gonntp.Article{
		Header: h,
		Body:   io.NopCloser(bytes.NewReader(body)),
		Bytes:  len(body),
	}, nil
}
func (b *asmBackend) GetArticles(group *gonntp.Group, from, to int64) ([]nntpserver.NumberedArticle, error) {
	return nil, nil
}
func (b *asmBackend) Authorized() bool                                           { return true }
func (b *asmBackend) Authenticate(user, pass string) (nntpserver.Backend, error) { return b, nil }
func (b *asmBackend) AllowPost() bool                                            { return false }
func (b *asmBackend) Post(article *gonntp.Article) error                         { return nil }

func startAsmServer(backend *asmBackend) (string, func()) {
	srv := nntpserver.NewServer(backend)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	done := make(chan struct{})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-done:
					return
				default:
					continue
				}
			}
			go srv.Process(conn)
		}
	}()
	return ln.Addr().String(), func() { close(done); ln.Close() }
}

func TestDownloadAll_SingleFile(t *testing.T) {
	backend := newAsmBackend()

	// Create a 3-segment file.
	part1 := []byte("AAAA")
	part2 := []byte("BBBB")
	part3 := []byte("CC")

	backend.addArticle("seg1@test", makeYencArticle(part1, 1, 3, "movie.mkv"))
	backend.addArticle("seg2@test", makeYencArticle(part2, 2, 3, "movie.mkv"))
	backend.addArticle("seg3@test", makeYencArticle(part3, 3, 3, "movie.mkv"))

	addr, stop := startAsmServer(backend)
	defer stop()

	host, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	pool := nntp.NewPool(&nntp.Credentials{
		Host: host, Port: port,
		Username: "", Password: "",
		SSL: false, MaxConnections: 1,
	})
	defer pool.Close()

	// Mock server requires GROUP before BODY.
	c, _ := pool.Get()
	c.Group("alt.binaries.test")
	pool.Put(c)

	n := &nzb.NZB{
		Files: []nzb.File{{
			Subject: `"movie.mkv" (1/1)`,
			Groups:  []string{"alt.binaries.test"},
			Segments: []nzb.Segment{
				{MessageID: "seg1@test", Number: 1, Bytes: 100},
				{MessageID: "seg2@test", Number: 2, Bytes: 100},
				{MessageID: "seg3@test", Number: 3, Bytes: 50},
			},
		}},
	}

	tmpDir := t.TempDir()
	asm := assembler.New(pool)

	var lastProgress float64
	results, err := asm.DownloadAll(context.Background(), n, tmpDir, func(progress float64, bytes int64) {
		lastProgress = progress
	})
	if err != nil {
		t.Fatalf("DownloadAll failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	content, _ := os.ReadFile(results[0].Path)
	expected := append(append(part1, part2...), part3...)
	if !bytes.Equal(content, expected) {
		t.Fatalf("content mismatch: got %q, want %q", content, expected)
	}

	if lastProgress < 0.99 {
		t.Fatalf("progress should be ~1.0, got %f", lastProgress)
	}
}

func TestDownloadAll_ResumeSkipsCompleted(t *testing.T) {
	backend := newAsmBackend()
	backend.addArticle("r1@test", makeYencArticle([]byte("AAAA"), 1, 2, "movie.mkv"))
	backend.addArticle("r2@test", makeYencArticle([]byte("BBBB"), 2, 2, "movie.mkv"))

	addr, stop := startAsmServer(backend)
	defer stop()
	host, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	pool := nntp.NewPool(&nntp.Credentials{Host: host, Port: port, SSL: false, MaxConnections: 1})
	defer pool.Close()
	c, _ := pool.Get()
	c.Group("alt.binaries.test")
	pool.Put(c)

	n := &nzb.NZB{Files: []nzb.File{{
		Subject: `"movie.mkv" (1/1)`,
		Groups:  []string{"alt.binaries.test"},
		Segments: []nzb.Segment{
			{MessageID: "r1@test", Number: 1, Bytes: 100},
			{MessageID: "r2@test", Number: 2, Bytes: 100},
		},
	}}}

	tmpDir := t.TempDir()
	asm := assembler.New(pool)

	// First run: downloads + finalizes the file.
	if _, err := asm.DownloadAll(context.Background(), n, tmpDir, nil); err != nil {
		t.Fatalf("first run failed: %v", err)
	}

	// Wipe the articles: any re-fetch now fails. A correct resume must skip the
	// already-finished file and succeed without touching the network.
	backend.articles = map[string][]byte{}

	results, err := asm.DownloadAll(context.Background(), n, tmpDir, nil)
	if err != nil {
		t.Fatalf("resume run should skip completed file, but failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result on resume, got %d", len(results))
	}
	content, _ := os.ReadFile(results[0].Path)
	if !bytes.Equal(content, []byte("AAAABBBB")) {
		t.Fatalf("resumed content mismatch: got %q", content)
	}
}

func TestDownloadAll_MultipleFiles(t *testing.T) {
	backend := newAsmBackend()

	backend.addArticle("vid1@test", makeYencArticle([]byte("video"), 1, 1, "video.mkv"))
	backend.addArticle("nfo1@test", makeYencArticle([]byte("info"), 1, 1, "info.nfo"))

	addr, stop := startAsmServer(backend)
	defer stop()

	host, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	pool := nntp.NewPool(&nntp.Credentials{
		Host: host, Port: port,
		SSL: false, MaxConnections: 1,
	})
	defer pool.Close()

	c, _ := pool.Get()
	c.Group("alt.binaries.test")
	pool.Put(c)

	n := &nzb.NZB{
		Files: []nzb.File{
			{
				Subject:  `"video.mkv" (1/1)`,
				Segments: []nzb.Segment{{MessageID: "vid1@test", Number: 1, Bytes: 100}},
			},
			{
				Subject:  `"info.nfo" (1/1)`,
				Segments: []nzb.Segment{{MessageID: "nfo1@test", Number: 1, Bytes: 50}},
			},
		},
	}

	tmpDir := t.TempDir()
	asm := assembler.New(pool)
	results, err := asm.DownloadAll(context.Background(), n, tmpDir, nil)
	if err != nil {
		t.Fatalf("DownloadAll failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

func TestDownloadAll_MissingArticle(t *testing.T) {
	backend := newAsmBackend()
	// Only add 1 of 2 segments.
	backend.addArticle("exists@test", makeYencArticle([]byte("data"), 1, 2, "file.bin"))

	addr, stop := startAsmServer(backend)
	defer stop()

	host, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	pool := nntp.NewPool(&nntp.Credentials{
		Host: host, Port: port,
		SSL: false, MaxConnections: 1,
	})
	defer pool.Close()

	c, _ := pool.Get()
	c.Group("alt.binaries.test")
	pool.Put(c)

	n := &nzb.NZB{
		Files: []nzb.File{{
			Subject: `"file.bin" (1/1)`,
			Segments: []nzb.Segment{
				{MessageID: "exists@test", Number: 1, Bytes: 100},
				{MessageID: "missing@test", Number: 2, Bytes: 100},
			},
		}},
	}

	tmpDir := t.TempDir()
	asm := assembler.New(pool)
	_, err := asm.DownloadAll(context.Background(), n, tmpDir, nil)
	if err == nil {
		t.Fatal("expected error for missing article")
	}
}

func TestDownloadAll_Cancellation(t *testing.T) {
	backend := newAsmBackend()
	backend.addArticle("s1@test", makeYencArticle([]byte("data"), 1, 1, "file.bin"))

	addr, stop := startAsmServer(backend)
	defer stop()

	host, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	pool := nntp.NewPool(&nntp.Credentials{
		Host: host, Port: port,
		SSL: false, MaxConnections: 3,
	})
	defer pool.Close()

	n := &nzb.NZB{
		Files: []nzb.File{{
			Subject:  `"file.bin" (1/1)`,
			Segments: []nzb.Segment{{MessageID: "s1@test", Number: 1, Bytes: 100}},
		}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	tmpDir := t.TempDir()
	asm := assembler.New(pool)
	_, err := asm.DownloadAll(ctx, n, tmpDir, nil)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}
