package nntp_test

import (
	"sync"
	"testing"
	"time"

	"github.com/torrin-app/torrin/internal/usenet/nntp"
)

func TestPool_GetPut(t *testing.T) {
	backend := newMockBackend("user", "pass")
	backend.addArticle("test@pool.com", []byte("data"))
	addr, stop := startMockServer(backend)
	defer stop()

	host, port := splitAddr(t, addr)
	pool := nntp.NewPool(&nntp.Credentials{
		Host: host, Port: port,
		Username: "user", Password: "pass",
		SSL: false, MaxConnections: 3,
	})
	defer pool.Close()

	// Get a connection.
	conn, err := pool.Get()
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	active, idle, max := pool.Stats()
	if active != 1 {
		t.Fatalf("expected 1 active, got %d", active)
	}
	if idle != 0 {
		t.Fatalf("expected 0 idle, got %d", idle)
	}
	if max != 3 {
		t.Fatalf("expected max 3, got %d", max)
	}

	// Put it back.
	pool.Put(conn)

	active, idle, _ = pool.Stats()
	if active != 0 {
		t.Fatalf("expected 0 active after put, got %d", active)
	}
	if idle != 1 {
		t.Fatalf("expected 1 idle after put, got %d", idle)
	}
}

func TestPool_Reuse(t *testing.T) {
	backend := newMockBackend("user", "pass")
	addr, stop := startMockServer(backend)
	defer stop()

	host, port := splitAddr(t, addr)
	pool := nntp.NewPool(&nntp.Credentials{
		Host: host, Port: port,
		Username: "user", Password: "pass",
		SSL: false, MaxConnections: 3,
	})
	defer pool.Close()

	// Get and put back.
	c1, _ := pool.Get()
	pool.Put(c1)

	// Get again -- should reuse the same connection (LIFO).
	c2, _ := pool.Get()
	pool.Put(c2)

	// Only 1 connection should have been created.
	_, idle, _ := pool.Stats()
	if idle != 1 {
		t.Fatalf("expected 1 idle (reused), got %d", idle)
	}
}

func TestPool_ConcurrentAccess(t *testing.T) {
	backend := newMockBackend("user", "pass")
	backend.addArticle("concurrent@test.com", []byte("data"))
	addr, stop := startMockServer(backend)
	defer stop()

	host, port := splitAddr(t, addr)
	pool := nntp.NewPool(&nntp.Credentials{
		Host: host, Port: port,
		Username: "user", Password: "pass",
		SSL: false, MaxConnections: 5,
	})
	defer pool.Close()

	// Spawn 10 goroutines, each gets a connection and uses it.
	var wg sync.WaitGroup
	errors := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := pool.Get()
			if err != nil {
				errors <- err
				return
			}
			// Simulate work.
			time.Sleep(10 * time.Millisecond)
			pool.Put(conn)
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Fatalf("concurrent error: %v", err)
	}
}

func TestPool_Discard(t *testing.T) {
	backend := newMockBackend("user", "pass")
	addr, stop := startMockServer(backend)
	defer stop()

	host, port := splitAddr(t, addr)
	pool := nntp.NewPool(&nntp.Credentials{
		Host: host, Port: port,
		Username: "user", Password: "pass",
		SSL: false, MaxConnections: 3,
	})
	defer pool.Close()

	conn, _ := pool.Get()

	pool.Discard(conn)

	active, idle, _ := pool.Stats()
	if active != 0 {
		t.Fatalf("expected 0 active after discard, got %d", active)
	}
	if idle != 0 {
		t.Fatalf("expected 0 idle after discard, got %d", idle)
	}
}

func TestPool_CleanIdle(t *testing.T) {
	backend := newMockBackend("user", "pass")
	addr, stop := startMockServer(backend)
	defer stop()

	host, port := splitAddr(t, addr)
	pool := nntp.NewPool(&nntp.Credentials{
		Host: host, Port: port,
		Username: "user", Password: "pass",
		SSL: false, MaxConnections: 3,
	})
	defer pool.Close()

	conn, _ := pool.Get()
	pool.Put(conn)
	conn.LastUsed = time.Now().Add(-1 * time.Minute)

	_, idle, _ := pool.Stats()
	if idle != 1 {
		t.Fatalf("expected 1 idle before clean, got %d", idle)
	}

	pool.CleanIdle()

	_, idle, _ = pool.Stats()
	if idle != 0 {
		t.Fatalf("expected 0 idle after clean, got %d", idle)
	}
}

func TestPool_Close(t *testing.T) {
	backend := newMockBackend("user", "pass")
	addr, stop := startMockServer(backend)
	defer stop()

	host, port := splitAddr(t, addr)
	pool := nntp.NewPool(&nntp.Credentials{
		Host: host, Port: port,
		Username: "user", Password: "pass",
		SSL: false, MaxConnections: 3,
	})

	conn, _ := pool.Get()
	pool.Put(conn)
	pool.Close()

	// Get after close should fail.
	_, err := pool.Get()
	if err == nil {
		t.Fatal("expected error after pool close")
	}
}
