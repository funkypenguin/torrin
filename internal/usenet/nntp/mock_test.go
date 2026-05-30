package nntp_test

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"strings"

	gonntp "github.com/dustin/go-nntp"
	nntpserver "github.com/dustin/go-nntp/server"
)

// mockBackend is a fake NNTP server backend for testing.
type mockBackend struct {
	articles map[string][]byte // messageID -> raw body
	user     string
	pass     string
	authed   bool
}

func newMockBackend(user, pass string) *mockBackend {
	return &mockBackend{
		articles: make(map[string][]byte),
		user:     user,
		pass:     pass,
	}
}

func (b *mockBackend) addArticle(messageID string, body []byte) {
	b.articles[messageID] = body
}

func (b *mockBackend) ListGroups(max int) ([]*gonntp.Group, error) {
	return []*gonntp.Group{{Name: "alt.binaries.test", High: 1, Low: 1, Count: 1}}, nil
}

func (b *mockBackend) GetGroup(name string) (*gonntp.Group, error) {
	return &gonntp.Group{Name: name, High: 1, Low: 1, Count: 1}, nil
}

func (b *mockBackend) GetArticle(group *gonntp.Group, id string) (*gonntp.Article, error) {
	id = strings.TrimPrefix(id, "<")
	id = strings.TrimSuffix(id, ">")
	body, ok := b.articles[id]
	if !ok {
		return nil, fmt.Errorf("430 article not found: %s", id)
	}
	h := textproto.MIMEHeader{}
	h.Set("Message-Id", "<"+id+">")
	return &gonntp.Article{
		Header: h,
		Body:   io.NopCloser(bytes.NewReader(body)),
		Bytes:  len(body),
		Lines:  strings.Count(string(body), "\n"),
	}, nil
}

func (b *mockBackend) GetArticles(group *gonntp.Group, from, to int64) ([]nntpserver.NumberedArticle, error) {
	return nil, nil
}

func (b *mockBackend) Authorized() bool {
	return b.authed || (b.user == "" && b.pass == "")
}

func (b *mockBackend) Authenticate(user, pass string) (nntpserver.Backend, error) {
	if user == b.user && pass == b.pass {
		authed := &mockBackend{articles: b.articles, user: b.user, pass: b.pass, authed: true}
		return authed, nil
	}
	return nil, fmt.Errorf("auth failed")
}

func (b *mockBackend) AllowPost() bool                   { return false }
func (b *mockBackend) Post(article *gonntp.Article) error { return fmt.Errorf("no posting") }

// startMockServer starts a mock NNTP server and returns the address + cleanup func.
func startMockServer(backend *mockBackend) (string, func()) {
	srv := nntpserver.NewServer(backend)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}

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

	addr := ln.Addr().String()
	stop := func() {
		close(done)
		ln.Close()
	}
	return addr, stop
}
