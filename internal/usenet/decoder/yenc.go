package decoder

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

type YencResult struct {
	Data     []byte
	Filename string
	Part     int
	Total    int
	Size     int64
	Begin    int64
	End      int64
}

func Decode(data []byte) (*YencResult, error) {
	result := &YencResult{}

	normalized := bytes.ReplaceAll(data, []byte("\r\n"), []byte("\n"))
	lines := bytes.Split(normalized, []byte("\n"))

	var dataStart, dataEnd int
	foundBegin := false
	for i, line := range lines {
		if bytes.HasPrefix(line, []byte("=ybegin ")) {
			parseYencHeader(string(line), result)
			dataStart = i + 1
			foundBegin = true
		} else if bytes.HasPrefix(line, []byte("=ypart ")) {
			parseYpartHeader(string(line), result)
			dataStart = i + 1
		} else if bytes.HasPrefix(line, []byte("=yend ")) {
			dataEnd = i
			break
		}
	}

	if !foundBegin {
		return nil, fmt.Errorf("no yenc data found")
	}
	if dataEnd == 0 {
		dataEnd = len(lines)
	}
	if dataStart >= dataEnd {
		result.Data = []byte{}
		return result, nil
	}

	var buf bytes.Buffer
	const maxGrow = 2 * 1024 * 1024
	if result.Size > 0 && result.Size <= maxGrow {
		buf.Grow(int(result.Size))
	}

	for _, line := range lines[dataStart:dataEnd] {
		decodeLine(line, &buf)
	}

	result.Data = buf.Bytes()
	return result, nil
}

func decodeLine(line []byte, buf *bytes.Buffer) {
	i := 0
	for i < len(line) {
		b := line[i]
		if b == '=' && i+1 < len(line) {
			i++
			b = line[i] - 64
		}
		buf.WriteByte(b - 42)
		i++
	}
}

func parseYencHeader(line string, r *YencResult) {
	if v := extractValue(line, "name"); v != "" {
		r.Filename = v
	}
	if v := extractValue(line, "size"); v != "" {
		r.Size, _ = strconv.ParseInt(v, 10, 64)
	}
	if v := extractValue(line, "part"); v != "" {
		r.Part, _ = strconv.Atoi(v)
	}
	if v := extractValue(line, "total"); v != "" {
		r.Total, _ = strconv.Atoi(v)
	}
}

func parseYpartHeader(line string, r *YencResult) {
	if v := extractValue(line, "begin"); v != "" {
		r.Begin, _ = strconv.ParseInt(v, 10, 64)
	}
	if v := extractValue(line, "end"); v != "" {
		r.End, _ = strconv.ParseInt(v, 10, 64)
	}
}

func extractValue(line, key string) string {
	prefix := key + "="
	idx := strings.Index(line, prefix)
	if idx < 0 {
		return ""
	}
	start := idx + len(prefix)
	if key == "name" {
		return strings.TrimSpace(line[start:])
	}
	end := strings.IndexByte(line[start:], ' ')
	if end < 0 {
		return strings.TrimSpace(line[start:])
	}
	return line[start : start+end]
}
