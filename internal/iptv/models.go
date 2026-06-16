package iptv

type Stream struct {
	StreamID           int    `json:"stream_id"`
	Name               string `json:"name"`
	ContainerExtension string `json:"container_extension"`
	CategoryID         string `json:"category_id"`
}

type ServerInfo struct {
	URL       string `json:"url"`
	Port      string `json:"port"`
	HTTPSPort string `json:"https_port"`
	Protocol  string `json:"server_protocol"`
}

type AuthResponse struct {
	UserInfo   map[string]any `json:"user_info"`
	ServerInfo ServerInfo     `json:"server_info"`
}

type SearchResult struct {
	StreamID  int
	Name      string
	Extension string
	StreamURL string
}

type Series struct {
	SeriesID int    `json:"series_id"`
	Name     string `json:"name"`
}

type LiveCategory struct {
	CategoryID   string `json:"category_id"`
	CategoryName string `json:"category_name"`
}

type LiveStream struct {
	StreamID   int    `json:"stream_id"`
	Name       string `json:"name"`
	StreamIcon string `json:"stream_icon"`
	CategoryID string `json:"category_id"`
}

type LiveChannel struct {
	StreamID int    `json:"stream_id"`
	Name     string `json:"name"`
	Logo     string `json:"logo"`
	Category string `json:"category"`
}

type Episode struct {
	ID                 string `json:"id"`
	EpisodeNum         string `json:"episode_num"`
	Title              string `json:"title"`
	ContainerExtension string `json:"container_extension"`
	Season             int    `json:"season"`
}

type SeriesInfo struct {
	Episodes map[string][]Episode `json:"episodes"`
}
