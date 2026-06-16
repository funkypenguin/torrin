package iptv

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Proxy struct {
	httpClient *http.Client
}

func NewProxy(proxyURL string) *Proxy {
	transport := &http.Transport{}
	if proxyURL != "" {
		proxyParsed, err := url.Parse(proxyURL)
		if err == nil {
			transport.Proxy = http.ProxyURL(proxyParsed)
		}
	}
	transport.ResponseHeaderTimeout = 30 * time.Second
	transport.DisableKeepAlives = true
	transport.IdleConnTimeout = 5 * time.Second
	return &Proxy{
		httpClient: &http.Client{
			Timeout:   4 * time.Hour,
			Transport: transport,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return nil
			},
		},
	}
}

type ValidateResult struct {
	Valid bool
	Size  int64
}

func (p *Proxy) Validate(ctx context.Context, streamURL string) ValidateResult {
	req, err := http.NewRequestWithContext(ctx, "GET", streamURL, nil)
	if err != nil {
		return ValidateResult{}
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")
	req.Header.Set("Range", "bytes=0-1000")
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return ValidateResult{}
	}
	defer resp.Body.Close()
	ct := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "video/") {
		return ValidateResult{}
	}
	cl := resp.ContentLength
	if cl != -1 && cl <= 100 {
		return ValidateResult{}
	}
	var fullSize int64
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		if idx := strings.LastIndex(cr, "/"); idx != -1 {
			fmt.Sscanf(cr[idx+1:], "%d", &fullSize)
		}
	}
	return ValidateResult{Valid: true, Size: fullSize}
}

func (p *Proxy) ServeStream(w http.ResponseWriter, r *http.Request, streamURL string) {
	req, err := http.NewRequestWithContext(r.Context(), "GET", streamURL, nil)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")

	start := time.Now()
	resp, err := p.httpClient.Do(req)
	if err != nil {
		slog.Error("iptv proxy fetch failed", "url", streamURL, "err", err)
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for _, h := range []string{"Content-Type", "Content-Length", "Content-Range", "Accept-Ranges"} {
		if v := resp.Header.Get(h); v != "" {
			w.Header().Set(h, v)
		}
	}
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Cross-Origin-Resource-Policy", "cross-origin")
	w.Header().Set("Cache-Control", "no-store")

	w.WriteHeader(resp.StatusCode)
	written, _ := io.Copy(w, resp.Body)

	slog.Info("iptv proxy", "bytes", written, "duration", time.Since(start).Round(time.Millisecond))
}
