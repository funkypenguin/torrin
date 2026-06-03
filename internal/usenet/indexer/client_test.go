package indexer

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

const sampleSearchResponse = `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:newznab="http://www.newznab.com/DTD/2010/feeds/attributes/">
  <channel>
    <newznab:response offset="0" total="2" />
    <item>
      <title>The.Matrix.1999.2160p.UHD.BluRay.x265-GROUP</title>
      <link>http://localhost/api?t=get&amp;id=abc123&amp;apikey=testkey</link>
      <guid>http://localhost/details/abc123</guid>
      <pubDate>Sat, 10 Feb 2024 14:30:00 +0000</pubDate>
      <enclosure url="http://localhost/api?t=get&amp;id=abc123&amp;apikey=testkey"
                 length="15728640000"
                 type="application/x-nzb" />
      <newznab:attr name="guid" value="abc123" />
      <newznab:attr name="size" value="15728640000" />
      <newznab:attr name="imdb" value="0133093" />
      <newznab:attr name="imdbtitle" value="The Matrix" />
      <newznab:attr name="imdbyear" value="1999" />
      <newznab:attr name="grabs" value="150" />
      <newznab:attr name="category" value="2045" />
    </item>
    <item>
      <title>The.Matrix.1999.1080p.BluRay.x264-OTHER</title>
      <link>http://localhost/api?t=get&amp;id=def456&amp;apikey=testkey</link>
      <guid>http://localhost/details/def456</guid>
      <pubDate>Fri, 09 Feb 2024 10:00:00 +0000</pubDate>
      <enclosure url="http://localhost/api?t=get&amp;id=def456&amp;apikey=testkey"
                 length="8000000000"
                 type="application/x-nzb" />
      <newznab:attr name="guid" value="def456" />
      <newznab:attr name="size" value="8000000000" />
      <newznab:attr name="imdb" value="0133093" />
      <newznab:attr name="grabs" value="300" />
      <newznab:attr name="category" value="2040" />
    </item>
  </channel>
</rss>`

const sampleErrorResponse = `<?xml version="1.0" encoding="UTF-8"?>
<error code="100" description="Incorrect user credentials" />`

const sampleEmptyResponse = `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:newznab="http://www.newznab.com/DTD/2010/feeds/attributes/">
  <channel>
    <newznab:response offset="0" total="0" />
  </channel>
</rss>`

func TestSearchMovie(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("t") != "movie" {
			t.Fatalf("expected t=movie, got %s", r.URL.Query().Get("t"))
		}
		if r.URL.Query().Get("imdbid") != "0133093" {
			t.Fatalf("expected imdbid=0133093, got %s", r.URL.Query().Get("imdbid"))
		}
		if r.URL.Query().Get("apikey") != "testkey" {
			t.Fatalf("expected apikey=testkey, got %s", r.URL.Query().Get("apikey"))
		}
		w.Header().Set("Content-Type", "application/xml")
		w.Write([]byte(sampleSearchResponse))
	}))
	defer srv.Close()

	client := NewTestClient(srv.URL, "testkey")
	results, err := client.SearchMovie("tt0133093")
	if err != nil {
		t.Fatalf("SearchMovie failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	r := results[0]
	if r.ID != "abc123" {
		t.Fatalf("expected ID abc123, got %s", r.ID)
	}
	if r.Title != "The.Matrix.1999.2160p.UHD.BluRay.x265-GROUP" {
		t.Fatalf("unexpected title: %s", r.Title)
	}
	if r.Size != 15728640000 {
		t.Fatalf("expected size 15728640000, got %d", r.Size)
	}
	if r.IMDBID != "0133093" {
		t.Fatalf("expected IMDB 0133093, got %s", r.IMDBID)
	}
	if r.IMDBTitle != "The Matrix" {
		t.Fatalf("expected IMDBTitle 'The Matrix', got %s", r.IMDBTitle)
	}
	if r.IMDBYear != 1999 {
		t.Fatalf("expected year 1999, got %d", r.IMDBYear)
	}
	if r.Grabs != 150 {
		t.Fatalf("expected 150 grabs, got %d", r.Grabs)
	}
	if r.Category != "2045" {
		t.Fatalf("expected category 2045, got %s", r.Category)
	}
}

func TestSearchMovie_StripsTTPrefix(t *testing.T) {
	var receivedIMDB string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedIMDB = r.URL.Query().Get("imdbid")
		w.Write([]byte(sampleEmptyResponse))
	}))
	defer srv.Close()

	client := NewTestClient(srv.URL, "key")
	client.SearchMovie("tt1234567")
	if receivedIMDB != "1234567" {
		t.Fatalf("expected tt prefix stripped, got %s", receivedIMDB)
	}
}

func TestSearchTV(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("t") != "tvsearch" {
			t.Fatalf("expected t=tvsearch, got %s", r.URL.Query().Get("t"))
		}
		if r.URL.Query().Get("imdbid") != "0944947" {
			t.Fatalf("expected imdbid=0944947, got %s", r.URL.Query().Get("imdbid"))
		}
		if r.URL.Query().Get("season") != "1" {
			t.Fatalf("expected season=1, got %s", r.URL.Query().Get("season"))
		}
		if r.URL.Query().Get("ep") != "3" {
			t.Fatalf("expected ep=3, got %s", r.URL.Query().Get("ep"))
		}
		w.Write([]byte(sampleEmptyResponse))
	}))
	defer srv.Close()

	client := NewTestClient(srv.URL, "key")
	results, err := client.SearchTV("tt0944947", 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestSearchQuery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("t") != "search" {
			t.Fatalf("expected t=search, got %s", r.URL.Query().Get("t"))
		}
		if r.URL.Query().Get("q") != "The Matrix 1999" {
			t.Fatalf("expected q='The Matrix 1999', got %s", r.URL.Query().Get("q"))
		}
		w.Write([]byte(sampleSearchResponse))
	}))
	defer srv.Close()

	client := NewTestClient(srv.URL, "key")
	results, err := client.SearchQuery("The Matrix 1999", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

func TestSearch_APIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(sampleErrorResponse))
	}))
	defer srv.Close()

	client := NewTestClient(srv.URL, "badkey")
	_, err := client.SearchMovie("0133093")
	if err == nil {
		t.Fatal("expected error for bad API key")
	}
	if err.Error() != "indexer api error 100: Incorrect user credentials" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSearch_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte("internal error"))
	}))
	defer srv.Close()

	client := NewTestClient(srv.URL, "key")
	_, err := client.SearchMovie("0133093")
	if err == nil {
		t.Fatal("expected error for HTTP 500")
	}
}

func TestDownloadNZB(t *testing.T) {
	nzbContent := `<?xml version="1.0"?><nzb><file></file></nzb>`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("t") != "get" {
			t.Fatalf("expected t=get, got %s", r.URL.Query().Get("t"))
		}
		if r.URL.Query().Get("id") != "abc123" {
			t.Fatalf("expected id=abc123, got %s", r.URL.Query().Get("id"))
		}
		w.Write([]byte(nzbContent))
	}))
	defer srv.Close()

	client := NewTestClient(srv.URL, "key")
	data, err := client.DownloadNZB(&Result{ID: "abc123"})
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != nzbContent {
		t.Fatalf("unexpected NZB content: %s", data)
	}
}

func TestDownloadNZB_UsesNZBURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("nzb data"))
	}))
	defer srv.Close()

	client := NewTestClient("http://other.host", "key")
	data, err := client.DownloadNZB(&Result{ID: "x", NZBURL: srv.URL + "/custom"})
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "nzb data" {
		t.Fatalf("unexpected: %s", data)
	}
}

func TestSearch_EmptyResults(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(sampleEmptyResponse))
	}))
	defer srv.Close()

	client := NewTestClient(srv.URL, "key")
	results, err := client.SearchMovie("9999999")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestExtractGUID(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"https://indexer.com/details/abc123", "abc123"},
		{"https://indexer.com/details/abc123/", "abc123"},
		{"abc123", "abc123"},
	}
	for _, tt := range tests {
		got := extractGUID(tt.input)
		if got != tt.want {
			t.Errorf("extractGUID(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestParseDate(t *testing.T) {
	d := parseDate("Sat, 10 Feb 2024 14:30:00 +0000")
	if d.Year() != 2024 || d.Month() != 2 || d.Day() != 10 {
		t.Fatalf("unexpected date: %v", d)
	}

	empty := parseDate("invalid")
	if !empty.IsZero() {
		t.Fatal("expected zero time for invalid date")
	}
}
