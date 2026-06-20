package rclonerc

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// a mock rcd that records the last request and replies with a canned result/status.
type mockRCD struct {
	lastPath string
	lastBody map[string]any
	status   int
	reply    string
}

func (m *mockRCD) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.lastPath = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		m.lastBody = map[string]any{}
		_ = json.Unmarshal(b, &m.lastBody)
		if m.status == 0 {
			m.status = 200
		}
		w.WriteHeader(m.status)
		if m.reply != "" {
			io.WriteString(w, m.reply)
		}
	}
}

func TestCreateRemote_sendsCorrectRC(t *testing.T) {
	m := &mockRCD{reply: "{}"}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	c := New(srv.URL)

	err := c.CreateRemote(context.Background(), "u_abc", "mega",
		map[string]string{"user": "a@b.com", "pass": "secret"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if m.lastPath != "/config/create" {
		t.Fatalf("path = %q, want /config/create", m.lastPath)
	}
	if m.lastBody["name"] != "u_abc" || m.lastBody["type"] != "mega" {
		t.Fatalf("bad body: %v", m.lastBody)
	}
	opt, _ := m.lastBody["opt"].(map[string]any)
	if opt["obscure"] != true {
		t.Fatalf("obscure not set: %v", m.lastBody["opt"])
	}
}

func TestCopyFile_sendsCorrectRC(t *testing.T) {
	m := &mockRCD{reply: "{}"}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	c := New(srv.URL)

	if err := c.CopyFile(context.Background(), "r2:", "hash/file.mkv", "u_abc:", "Torrin/file.mkv"); err != nil {
		t.Fatal(err)
	}
	if m.lastPath != "/operations/copyfile" {
		t.Fatalf("path = %q", m.lastPath)
	}
	if m.lastBody["srcFs"] != "r2:" || m.lastBody["dstFs"] != "u_abc:" {
		t.Fatalf("bad body: %v", m.lastBody)
	}
}

func TestCall_surfacesRcError(t *testing.T) {
	m := &mockRCD{status: 500, reply: `{"error":"didn't find section in config file"}`}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()
	c := New(srv.URL)

	err := c.CheckAccess(context.Background(), "u_missing:")
	if err == nil {
		t.Fatal("expected error from 500 response")
	}
	if got := err.Error(); got == "" || !contains(got, "didn't find section") {
		t.Fatalf("error should surface rc message, got: %q", got)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
