package rss

import (
	"context"
	"regexp"
	"strings"
	"time"

	"github.com/mmcdole/gofeed"
)

type Feed struct {
	ID     string
	UserID string
	URL    string
	Filter string
}

type Item struct {
	GUID   string
	Title  string
	Magnet string // torrent feeds
	NzbURL string // usenet feeds
}

func FetchFeed(ctx context.Context, feedURL string) ([]Item, error) {
	fp := gofeed.NewParser()
	fp.UserAgent = "Torrin/1.0"

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	feed, err := fp.ParseURLWithContext(feedURL, ctx)
	if err != nil {
		return nil, err
	}

	var items []Item
	for _, entry := range feed.Items {
		magnet := extractMagnet(entry)
		nzbURL := ""
		if magnet == "" {
			nzbURL = extractNZB(entry)
		}
		if magnet == "" && nzbURL == "" {
			continue
		}
		guid := entry.GUID
		if guid == "" {
			guid = entry.Link
		}
		if guid == "" {
			if magnet != "" {
				guid = magnet
			} else {
				guid = nzbURL
			}
		}
		items = append(items, Item{
			GUID:   guid,
			Title:  entry.Title,
			Magnet: magnet,
			NzbURL: nzbURL,
		})
	}
	return items, nil
}

func extractNZB(entry *gofeed.Item) string {
	for _, enc := range entry.Enclosures {
		if strings.Contains(strings.ToLower(enc.Type), "nzb") ||
			strings.Contains(strings.ToLower(enc.URL), ".nzb") {
			return enc.URL
		}
	}
	if strings.Contains(strings.ToLower(entry.Link), ".nzb") {
		return entry.Link
	}
	return ""
}

var magnetRegex = regexp.MustCompile(`magnet:\?xt=urn:btih:[a-zA-Z0-9]+[^\s"<>]*`)

func extractMagnet(entry *gofeed.Item) string {
	// Check enclosures first.
	for _, enc := range entry.Enclosures {
		if strings.HasPrefix(enc.URL, "magnet:") {
			return enc.URL
		}
	}

	// Check link.
	if strings.HasPrefix(entry.Link, "magnet:") {
		return entry.Link
	}

	// Search in description/content.
	for _, text := range []string{entry.Description, entry.Content} {
		if m := magnetRegex.FindString(text); m != "" {
			return m
		}
	}

	return ""
}

// MatchesFilter checks if an item title matches the filter string.
// Filter is a comma-separated list of keywords. Item must match at least one.
// Empty filter matches everything.
func MatchesFilter(title, filter string) bool {
	if filter == "" {
		return true
	}
	lower := strings.ToLower(title)
	for _, kw := range strings.Split(filter, ",") {
		kw = strings.TrimSpace(strings.ToLower(kw))
		if kw != "" && strings.Contains(lower, kw) {
			return true
		}
	}
	return false
}
