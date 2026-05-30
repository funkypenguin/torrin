package nntp_test

import (
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/torrin-app/torrin/internal/usenet/nntp"
)

func TestDial_Success(t *testing.T) {
	backend := newMockBackend("testuser", "testpass")
	backend.addArticle("test@example.com", []byte("hello world"))
	addr, stop := startMockServer(backend)
	defer stop()

	host, port := splitAddr(t, addr)
	creds := &nntp.Credentials{
		Host:           host,
		Port:           port,
		Username:       "testuser",
		Password:       "testpass",
		SSL:            false,
		MaxConnections: 5,
	}

	conn, err := nntp.Dial(creds)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()
}

func TestDial_BadCredentials(t *testing.T) {
	backend := newMockBackend("testuser", "testpass")
	addr, stop := startMockServer(backend)
	defer stop()

	host, port := splitAddr(t, addr)
	creds := &nntp.Credentials{
		Host:     host,
		Port:     port,
		Username: "wrong",
		Password: "wrong",
		SSL:      false,
	}

	_, err := nntp.Dial(creds)
	if err == nil {
		t.Fatal("expected auth error, got nil")
	}
	if !strings.Contains(err.Error(), "auth") {
		t.Fatalf("expected auth error, got: %v", err)
	}
}

func TestConn_Body(t *testing.T) {
	backend := newMockBackend("user", "pass")
	backend.addArticle("article1@test.com", []byte("segment data here"))
	addr, stop := startMockServer(backend)
	defer stop()

	conn := dial(t, addr, "user", "pass")
	defer conn.Close()

	// Mock server requires GROUP before BODY.
	conn.Group("alt.binaries.test")

	body, err := conn.Body("article1@test.com")
	if err != nil {
		t.Fatalf("Body failed: %v", err)
	}
	if !strings.Contains(string(body), "segment data here") {
		t.Fatalf("unexpected body: %q", body)
	}
}

func TestConn_Body_NotFound(t *testing.T) {
	backend := newMockBackend("user", "pass")
	addr, stop := startMockServer(backend)
	defer stop()

	conn := dial(t, addr, "user", "pass")
	defer conn.Close()

	_, err := conn.Body("nonexistent@test.com")
	if err == nil {
		t.Fatal("expected error for missing article")
	}
}

func TestConn_Stat(t *testing.T) {
	backend := newMockBackend("user", "pass")
	backend.addArticle("exists@test.com", []byte("data"))
	addr, stop := startMockServer(backend)
	defer stop()

	conn := dial(t, addr, "user", "pass")
	defer conn.Close()

	err := conn.Stat("exists@test.com")
	_ = err
}

func TestConn_Ping(t *testing.T) {
	backend := newMockBackend("user", "pass")
	addr, stop := startMockServer(backend)
	defer stop()

	conn := dial(t, addr, "user", "pass")
	defer conn.Close()

	_ = conn.Ping()
}

func TestConn_Close(t *testing.T) {
	backend := newMockBackend("user", "pass")
	addr, stop := startMockServer(backend)
	defer stop()

	conn := dial(t, addr, "user", "pass")
	err := conn.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Should fail after close.
	_, err = conn.Body("anything@test.com")
	if err == nil {
		t.Fatal("expected error after close")
	}
}

// helpers

func splitAddr(t *testing.T, addr string) (string, int) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	var port int
	fmt.Sscanf(portStr, "%d", &port)
	return host, port
}

func dial(t *testing.T, addr, user, pass string) *nntp.Conn {
	t.Helper()
	host, port := splitAddr(t, addr)
	conn, err := nntp.Dial(&nntp.Credentials{
		Host: host, Port: port,
		Username: user, Password: pass,
		SSL: false, MaxConnections: 5,
	})
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	return conn
}
