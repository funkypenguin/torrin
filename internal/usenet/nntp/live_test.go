package nntp_test

import (
	"os"
	"testing"

	"github.com/torrin-app/torrin/internal/usenet/nntp"
)

// Live tests run against a real NNTP server.
// Set NNTP_HOST, NNTP_USER, NNTP_PASS env vars to enable.
// Example: NNTP_HOST=news.usenet.farm NNTP_USER=xxx NNTP_PASS=yyy go test -run TestLive

func skipIfNoLive(t *testing.T) {
	if os.Getenv("NNTP_HOST") == "" {
		t.Skip("set NNTP_HOST/NNTP_USER/NNTP_PASS to run live tests")
	}
}

func liveCreds() *nntp.Credentials {
	return &nntp.Credentials{
		Host:           os.Getenv("NNTP_HOST"),
		Port:           563,
		Username:       os.Getenv("NNTP_USER"),
		Password:       os.Getenv("NNTP_PASS"),
		SSL:            true,
		MaxConnections: 5,
	}
}

func TestLive_Dial(t *testing.T) {
	skipIfNoLive(t)

	conn, err := nntp.Dial(liveCreds())
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()
	t.Log("connected and authenticated successfully")
}

func TestLive_Ping(t *testing.T) {
	skipIfNoLive(t)

	conn, err := nntp.Dial(liveCreds())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.Ping(); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
	t.Log("ping successful")
}

func TestLive_Group(t *testing.T) {
	skipIfNoLive(t)

	conn, err := nntp.Dial(liveCreds())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.Group("alt.binaries.test"); err != nil {
		t.Fatalf("Group failed: %v", err)
	}
	t.Log("group selected successfully")
}

func TestLive_Pool(t *testing.T) {
	skipIfNoLive(t)

	pool := nntp.NewPool(liveCreds())
	defer pool.Close()

	c1, err := pool.Get()
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	c2, err := pool.Get()
	if err != nil {
		t.Fatalf("second Get failed: %v", err)
	}

	active, idle, max := pool.Stats()
	t.Logf("active=%d idle=%d max=%d", active, idle, max)
	if active != 2 {
		t.Fatalf("expected 2 active, got %d", active)
	}

	pool.Put(c1)
	pool.Put(c2)

	active, idle, _ = pool.Stats()
	if idle != 2 {
		t.Fatalf("expected 2 idle, got %d", idle)
	}
	t.Log("pool working correctly")
}
