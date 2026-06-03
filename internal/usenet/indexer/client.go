package indexer

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
	allowLocal bool
}

type Result struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	Size      int64     `json:"size"`
	PubDate   time.Time `json:"pub_date"`
	NZBURL    string    `json:"nzb_url"`
	Category  string    `json:"category"`
	IMDBID    string    `json:"imdb_id,omitempty"`
	IMDBTitle string    `json:"imdb_title,omitempty"`
	IMDBYear  int       `json:"imdb_year,omitempty"`
	Grabs     int       `json:"grabs"`
}

func NewClient(baseURL, apiKey string) *Client {
	return NewClientWithProxy(baseURL, apiKey, "")
}

// NewClientWithProxy creates a client that routes through an HTTP proxy.
func NewClientWithProxy(baseURL, apiKey, proxyURL string) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if proxyURL != "" {
		if p, err := url.Parse(proxyURL); err == nil {
			transport.Proxy = http.ProxyURL(p)
		}
	}
	// Block connections to private/internal IPs (SSRF prevention).
	// Skipped when allowLocal is set (tests only).
	transport.DialContext = ssrfSafeDialer(false)
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		httpClient: &http.Client{
			Timeout:   15 * time.Second,
			Transport: transport,
		},
	}
}

// ValidateURL checks that a URL is safe to fetch (https only, no private IPs).
func ValidateURL(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid URL")
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return fmt.Errorf("only http/https allowed")
	}
	host := u.Hostname()
	ips, err := net.LookupHost(host)
	if err != nil {
		return fmt.Errorf("DNS lookup failed: %w", err)
	}
	for _, ipStr := range ips {
		ip := net.ParseIP(ipStr)
		if ip == nil {
			continue
		}
		if isPrivateIP(ip) {
			return fmt.Errorf("private/internal addresses not allowed")
		}
	}
	return nil
}

// NewTestClient creates a client that allows localhost connections (for tests only).
func NewTestClient(baseURL, apiKey string) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		apiKey:     apiKey,
		allowLocal: true,
		httpClient: &http.Client{Timeout: 15 * time.Second, Transport: transport},
	}
}

func ssrfSafeDialer(allowLocal bool) func(ctx context.Context, network, addr string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		if allowLocal {
			return dialer.DialContext(ctx, network, addr)
		}
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		ips, err := net.DefaultResolver.LookupHost(ctx, host)
		if err != nil {
			return nil, err
		}
		for _, ipStr := range ips {
			ip := net.ParseIP(ipStr)
			if ip != nil && isPrivateIP(ip) {
				return nil, fmt.Errorf("connection to private address %s blocked", ipStr)
			}
		}
		return dialer.DialContext(ctx, network, net.JoinHostPort(ips[0], port))
	}
}

func isPrivateIP(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsUnspecified() ||
		ip.Equal(net.IPv4(169, 254, 169, 254)) // cloud metadata
}

func (c *Client) SearchMovie(imdbID string) ([]Result, error) {
	imdbID = strings.TrimPrefix(imdbID, "tt")
	params := url.Values{
		"t":        {"movie"},
		"imdbid":   {imdbID},
		"cat":      {"2000,2040,2045,2050"},
		"extended": {"1"},
		"limit":    {"50"},
		"apikey":   {c.apiKey},
	}
	return c.search(params)
}

func (c *Client) SearchTV(imdbID string, season, episode int) ([]Result, error) {
	imdbID = strings.TrimPrefix(imdbID, "tt")
	params := url.Values{
		"t":        {"tvsearch"},
		"imdbid":   {imdbID},
		"season":   {strconv.Itoa(season)},
		"ep":       {strconv.Itoa(episode)},
		"cat":      {"5000,5040,5045"},
		"extended": {"1"},
		"limit":    {"50"},
		"apikey":   {c.apiKey},
	}
	return c.search(params)
}

func (c *Client) SearchQuery(query string, categories string) ([]Result, error) {
	if categories == "" {
		categories = "2000,5000"
	}
	params := url.Values{
		"t":        {"search"},
		"q":        {query},
		"cat":      {categories},
		"extended": {"1"},
		"limit":    {"50"},
		"apikey":   {c.apiKey},
	}
	return c.search(params)
}

func (c *Client) DownloadNZB(result *Result) ([]byte, error) {
	nzbURL := result.NZBURL
	if nzbURL == "" {
		nzbURL = fmt.Sprintf("%s/api?t=get&id=%s&apikey=%s", c.baseURL, result.ID, c.apiKey)
	}

	resp, err := c.httpClient.Get(nzbURL)
	if err != nil {
		return nil, fmt.Errorf("download nzb: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("download nzb: status %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

func (c *Client) search(params url.Values) ([]Result, error) {
	reqURL := fmt.Sprintf("%s/api?%s", c.baseURL, params.Encode())

	resp, err := c.httpClient.Get(reqURL)
	if err != nil {
		return nil, fmt.Errorf("indexer request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("indexer error (%d): %s", resp.StatusCode, body)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	var apiErr apiError
	if xml.Unmarshal(body, &apiErr) == nil && apiErr.Code != 0 {
		return nil, fmt.Errorf("indexer api error %d: %s", apiErr.Code, apiErr.Description)
	}

	var rss rssResponse
	if err := xml.Unmarshal(body, &rss); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	var results []Result
	for _, item := range rss.Channel.Items {
		r := Result{
			Title:   item.Title,
			NZBURL:  item.Link,
			PubDate: parseDate(item.PubDate),
		}

		if item.Enclosure.URL != "" {
			r.NZBURL = item.Enclosure.URL
			r.Size = item.Enclosure.Length
		}

		for _, attr := range item.Attrs {
			switch attr.Name {
			case "guid":
				r.ID = attr.Value
			case "size":
				if s, err := strconv.ParseInt(attr.Value, 10, 64); err == nil {
					r.Size = s
				}
			case "category":
				r.Category = attr.Value
			case "imdb":
				r.IMDBID = attr.Value
			case "imdbtitle":
				r.IMDBTitle = attr.Value
			case "imdbyear":
				if y, err := strconv.Atoi(attr.Value); err == nil {
					r.IMDBYear = y
				}
			case "grabs":
				if g, err := strconv.Atoi(attr.Value); err == nil {
					r.Grabs = g
				}
			}
		}

		if r.ID == "" {
			r.ID = extractGUID(item.GUID.Value)
		}

		results = append(results, r)
	}

	return results, nil
}

func parseDate(s string) time.Time {
	formats := []string{
		time.RFC1123Z,
		time.RFC1123,
		"Mon, 02 Jan 2006 15:04:05 -0700",
		"Mon, 02 Jan 2006 15:04:05 MST",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

func extractGUID(link string) string {
	// Extract ID from URLs like https://indexer.com/details/abc123
	parts := strings.Split(strings.TrimRight(link, "/"), "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return link
}

type apiError struct {
	XMLName     xml.Name `xml:"error"`
	Code        int      `xml:"code,attr"`
	Description string   `xml:"description,attr"`
}

type rssResponse struct {
	XMLName xml.Name   `xml:"rss"`
	Channel rssChannel `xml:"channel"`
}

type rssChannel struct {
	Items []rssItem `xml:"item"`
}

type rssItem struct {
	Title     string       `xml:"title"`
	Link      string       `xml:"link"`
	GUID      rssGUID      `xml:"guid"`
	PubDate   string       `xml:"pubDate"`
	Enclosure rssEnclosure `xml:"enclosure"`
	Attrs     []nzbAttr    `xml:"http://www.newznab.com/DTD/2010/feeds/attributes/ attr"`
}

type rssGUID struct {
	Value string `xml:",chardata"`
}

type rssEnclosure struct {
	URL    string `xml:"url,attr"`
	Length int64  `xml:"length,attr"`
	Type   string `xml:"type,attr"`
}

type nzbAttr struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr"`
}
