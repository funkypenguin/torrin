package nntp

import (
	"fmt"
	"sync"
	"time"
)

const (
	idleTimeout  = 20 * time.Second
	staleTimeout = 10 * time.Second
)

type Pool struct {
	creds    *Credentials
	mu       sync.Mutex
	idle     []*Conn
	active   int
	maxConns int
	sem      chan struct{}
	closed   bool
}

func NewPool(creds *Credentials) *Pool {
	maxConns := creds.MaxConnections
	if maxConns <= 0 {
		maxConns = 5
	}
	if maxConns > 10 {
		maxConns = 10
	}
	return &Pool{
		creds:    creds,
		maxConns: maxConns,
		sem:      make(chan struct{}, maxConns),
	}
}

func (p *Pool) Get() (*Conn, error) {
	p.sem <- struct{}{}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		<-p.sem
		return nil, fmt.Errorf("pool closed")
	}

	for len(p.idle) > 0 {
		c := p.idle[len(p.idle)-1]
		p.idle = p.idle[:len(p.idle)-1]
		p.active++
		p.mu.Unlock()

		if time.Since(c.LastUsed) > staleTimeout {
			if err := c.Ping(); err != nil {
				c.Close()
				p.mu.Lock()
				p.active--
				p.mu.Unlock()
				p.mu.Lock()
				continue
			}
		}
		return c, nil
	}
	p.active++
	p.mu.Unlock()

	c, err := Dial(p.creds)
	if err != nil {
		p.mu.Lock()
		p.active--
		p.mu.Unlock()
		<-p.sem
		return nil, err
	}
	return c, nil
}

func (p *Pool) Put(c *Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.active--
	<-p.sem

	if p.closed {
		c.Close()
		return
	}

	c.LastUsed = time.Now()
	p.idle = append(p.idle, c)
}

func (p *Pool) Discard(c *Conn) {
	c.Close()
	p.mu.Lock()
	p.active--
	p.mu.Unlock()
	<-p.sem
}

func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	for _, c := range p.idle {
		c.Close()
	}
	p.idle = nil
}

func (p *Pool) MaxConns() int {
	return p.maxConns
}

func (p *Pool) CleanIdle() {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	kept := p.idle[:0]
	for _, c := range p.idle {
		if now.Sub(c.LastUsed) > idleTimeout {
			c.Close()
		} else {
			kept = append(kept, c)
		}
	}
	p.idle = kept
}

func (p *Pool) Stats() (active, idle, max int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.active, len(p.idle), p.maxConns
}
