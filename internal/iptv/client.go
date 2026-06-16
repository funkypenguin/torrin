package iptv

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/sahilm/fuzzy"
)

type Client struct {
	apiURL     string
	username   string
	password   string
	httpClient *http.Client
	serverInfo *ServerInfo

	cache      []Stream
	cacheNames []string
	cacheMu    sync.RWMutex
	cacheTime  time.Time

	seriesCache []Series
	seriesNames []string
	seriesMu    sync.RWMutex
	seriesTime  time.Time

	liveCache []LiveChannel
	liveMu    sync.RWMutex
	liveTime  time.Time
}

func NewClient(apiURL, username, password string) *Client {
	return &Client{
		apiURL:   strings.TrimRight(apiURL, "/"),
		username: username,
		password: password,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *Client) api(ctx context.Context, action string) (*http.Response, error) {
	url := fmt.Sprintf("%s/player_api.php?username=%s&password=%s&action=%s",
		c.apiURL, c.username, c.password, action)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	return c.httpClient.Do(req)
}

func (c *Client) Init(ctx context.Context) error {
	url := fmt.Sprintf("%s/player_api.php?username=%s&password=%s", c.apiURL, c.username, c.password)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var auth AuthResponse
	if err := json.NewDecoder(resp.Body).Decode(&auth); err != nil {
		return err
	}
	c.serverInfo = &auth.ServerInfo
	return nil
}

func (c *Client) loadCatalog(ctx context.Context) error {
	c.cacheMu.RLock()
	if time.Since(c.cacheTime) < 6*time.Hour && len(c.cache) > 0 {
		c.cacheMu.RUnlock()
		return nil
	}
	c.cacheMu.RUnlock()

	resp, err := c.api(ctx, "get_vod_streams")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var streams []Stream
	if err := json.NewDecoder(resp.Body).Decode(&streams); err != nil {
		return err
	}

	names := make([]string, len(streams))
	for i, s := range streams {
		names[i] = s.Name
	}

	c.cacheMu.Lock()
	c.cache = streams
	c.cacheNames = names
	c.cacheTime = time.Now()
	c.cacheMu.Unlock()

	return nil
}

func (c *Client) Search(ctx context.Context, query string) ([]SearchResult, error) {
	if err := c.loadCatalog(ctx); err != nil {
		return nil, err
	}

	if c.serverInfo == nil {
		if err := c.Init(ctx); err != nil {
			return nil, err
		}
	}

	c.cacheMu.RLock()
	defer c.cacheMu.RUnlock()

	matches := fuzzy.Find(query, c.cacheNames)

	var results []SearchResult
	for i, m := range matches {
		if i >= 10 {
			break
		}
		s := c.cache[m.Index]
		results = append(results, SearchResult{
			StreamID:  s.StreamID,
			Name:      s.Name,
			Extension: s.ContainerExtension,
			StreamURL: c.streamURL(s.StreamID, s.ContainerExtension),
		})
	}

	return results, nil
}

func (c *Client) loadSeriesCatalog(ctx context.Context) error {
	c.seriesMu.RLock()
	if time.Since(c.seriesTime) < 6*time.Hour && len(c.seriesCache) > 0 {
		c.seriesMu.RUnlock()
		return nil
	}
	c.seriesMu.RUnlock()

	resp, err := c.api(ctx, "get_series")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var series []Series
	if err := json.NewDecoder(resp.Body).Decode(&series); err != nil {
		return err
	}

	names := make([]string, len(series))
	for i, s := range series {
		names[i] = s.Name
	}

	c.seriesMu.Lock()
	c.seriesCache = series
	c.seriesNames = names
	c.seriesTime = time.Now()
	c.seriesMu.Unlock()

	return nil
}

func (c *Client) SeriesSample() []string {
	c.seriesMu.RLock()
	defer c.seriesMu.RUnlock()
	var sample []string
	for i, n := range c.seriesNames {
		if i >= 5 {
			break
		}
		sample = append(sample, n)
	}
	return sample
}

func (c *Client) SeriesCatalogSize() int {
	c.seriesMu.RLock()
	defer c.seriesMu.RUnlock()
	return len(c.seriesCache)
}

func (c *Client) SearchSeries(ctx context.Context, query string, season, episode int) ([]SearchResult, error) {
	if err := c.loadSeriesCatalog(ctx); err != nil {
		return nil, fmt.Errorf("load series catalog: %w", err)
	}

	if c.serverInfo == nil {
		if err := c.Init(ctx); err != nil {
			return nil, err
		}
	}

	c.seriesMu.RLock()
	seriesMatches := fuzzy.Find(query, c.seriesNames)

	var seriesIDs []int
	for i, m := range seriesMatches {
		if i >= 5 {
			break
		}
		seriesIDs = append(seriesIDs, c.seriesCache[m.Index].SeriesID)
	}
	c.seriesMu.RUnlock()

	if len(seriesIDs) == 0 {
		return nil, nil
	}

	seasonStr := fmt.Sprintf("%d", season)
	var results []SearchResult

	for _, sid := range seriesIDs {
		info, err := c.getSeriesInfo(ctx, sid)
		if err != nil || info == nil {
			continue
		}
		eps, ok := info.Episodes[seasonStr]
		if !ok {
			continue
		}
		for _, ep := range eps {
			if ep.EpisodeNum == fmt.Sprintf("%d", episode) {
				streamID := 0
				fmt.Sscanf(ep.ID, "%d", &streamID)
				if streamID == 0 {
					continue
				}
				results = append(results, SearchResult{
					StreamID:  streamID,
					Name:      ep.Title,
					Extension: ep.ContainerExtension,
					StreamURL: c.seriesStreamURL(streamID, ep.ContainerExtension),
				})
			}
		}
		if len(results) >= 3 {
			break
		}
	}

	return results, nil
}

func (c *Client) getSeriesInfo(ctx context.Context, seriesID int) (*SeriesInfo, error) {
	url := fmt.Sprintf("%s/player_api.php?username=%s&password=%s&action=get_series_info&series_id=%d",
		c.apiURL, c.username, c.password, seriesID)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var info SeriesInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, err
	}
	return &info, nil
}

func (c *Client) seriesStreamURL(streamID int, ext string) string {
	if c.serverInfo != nil {
		return fmt.Sprintf("http://%s:%s/series/%s/%s/%d.%s",
			c.serverInfo.URL, c.serverInfo.Port, c.username, c.password, streamID, ext)
	}
	return fmt.Sprintf("%s/series/%s/%s/%d.%s",
		c.apiURL, c.username, c.password, streamID, ext)
}

func (c *Client) GetSeriesStreamURL(streamID int, ext string) string {
	return c.seriesStreamURL(streamID, sanitizeExt(ext))
}

func (c *Client) GetStreamURL(streamID int, ext string) string {
	return c.streamURL(streamID, sanitizeExt(ext))
}

func sanitizeExt(ext string) string {
	var out []byte
	for _, c := range []byte(ext) {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			out = append(out, c)
		}
	}
	if len(out) == 0 {
		return "ts"
	}
	return string(out)
}

func (c *Client) streamURL(streamID int, ext string) string {
	if c.serverInfo != nil {
		return fmt.Sprintf("http://%s:%s/movie/%s/%s/%d.%s",
			c.serverInfo.URL, c.serverInfo.Port, c.username, c.password, streamID, ext)
	}
	return fmt.Sprintf("%s/movie/%s/%s/%d.%s",
		c.apiURL, c.username, c.password, streamID, ext)
}

func (c *Client) liveStreamURL(streamID int, ext string) string {
	if c.serverInfo != nil {
		return fmt.Sprintf("http://%s:%s/live/%s/%s/%d.%s",
			c.serverInfo.URL, c.serverInfo.Port, c.username, c.password, streamID, ext)
	}
	return fmt.Sprintf("%s/live/%s/%s/%d.%s",
		c.apiURL, c.username, c.password, streamID, ext)
}

func (c *Client) GetLiveStreamURL(streamID int, ext string) string {
	return c.liveStreamURL(streamID, "m3u8")
}

var sportsKeywords = []string{
	"sport", "nfl", "nba", "nhl", "mlb", "ncaa", "ufc", "ppv", "espn", "dazn",
	"bein", "formula", "f1", "motogp", "golf", "tennis", "soccer", "football",
	"rugby", "epl", "mls", "uefa", "bundesliga", "movistar liga", "fifa", "wnba",
	"setanta", "dirtvision", "gaago", "flo sport", "flocollege", "tsn", "stan sport",
	"fight", "fite", "wwe", "boxing", "league", "eurosport", "optus sport",
	"spark sport", "mola sport", "dyn sport", "magenta sport", "ohl", "whl", "qmjhl",
	"ahl", "sphl", "nrl", "big ten", "loi", "betting tv",
}

func isSportsCategory(name string) bool {
	n := strings.ToLower(name)
	for _, kw := range sportsKeywords {
		if strings.Contains(n, kw) {
			return true
		}
	}
	return false
}

func (c *Client) loadLiveSports(ctx context.Context) error {
	c.liveMu.RLock()
	if time.Since(c.liveTime) < 6*time.Hour && len(c.liveCache) > 0 {
		c.liveMu.RUnlock()
		return nil
	}
	c.liveMu.RUnlock()

	resp, err := c.api(ctx, "get_live_categories")
	if err != nil {
		return err
	}
	var cats []LiveCategory
	err = json.NewDecoder(resp.Body).Decode(&cats)
	resp.Body.Close()
	if err != nil {
		return err
	}

	sportsCats := map[string]string{}
	for _, cat := range cats {
		if isSportsCategory(cat.CategoryName) {
			sportsCats[cat.CategoryID] = cat.CategoryName
		}
	}

	respS, err := c.api(ctx, "get_live_streams")
	if err != nil {
		return err
	}
	var streams []LiveStream
	err = json.NewDecoder(respS.Body).Decode(&streams)
	respS.Body.Close()
	if err != nil {
		return err
	}

	var channels []LiveChannel
	for _, s := range streams {
		catName, ok := sportsCats[s.CategoryID]
		if !ok {
			continue
		}
		channels = append(channels, LiveChannel{
			StreamID: s.StreamID,
			Name:     s.Name,
			Logo:     s.StreamIcon,
			Category: catName,
		})
	}

	c.liveMu.Lock()
	c.liveCache = channels
	c.liveTime = time.Now()
	c.liveMu.Unlock()
	return nil
}

func (c *Client) SportsChannels(ctx context.Context) ([]LiveChannel, error) {
	if err := c.loadLiveSports(ctx); err != nil {
		return nil, err
	}
	c.liveMu.RLock()
	defer c.liveMu.RUnlock()
	out := make([]LiveChannel, len(c.liveCache))
	copy(out, c.liveCache)
	return out, nil
}

func (c *Client) CatalogSize() int {
	c.cacheMu.RLock()
	defer c.cacheMu.RUnlock()
	return len(c.cache)
}
