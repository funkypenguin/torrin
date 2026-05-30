package nzb

import (
	"fmt"
	"io"

	"github.com/Tensai75/nzbparser"
)

type NZB struct {
	Files []File
	Meta  map[string]string
	Raw   []byte
}

type File struct {
	Subject  string
	Filename string
	Groups   []string
	Segments []Segment
}

type Segment struct {
	MessageID string
	Number    int
	Bytes     int64
}

func Parse(r io.Reader) (*NZB, error) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read nzb: %w", err)
	}
	return ParseBytes(raw)
}

func ParseBytes(data []byte) (*NZB, error) {
	parsed, err := nzbparser.ParseString(string(data))
	if err != nil {
		return nil, fmt.Errorf("parse nzb: %w", err)
	}

	nzbparser.ScanNzbFile(parsed)

	n := &NZB{
		Raw:  data,
		Meta: parsed.Meta,
	}
	if n.Meta == nil {
		n.Meta = make(map[string]string)
	}

	for _, f := range parsed.Files {
		file := File{
			Subject:  f.Subject,
			Filename: f.Filename,
			Groups:   f.Groups,
		}
		for _, s := range f.Segments {
			file.Segments = append(file.Segments, Segment{
				MessageID: s.Id,
				Number:    s.Number,
				Bytes:     int64(s.Bytes),
			})
		}
		n.Files = append(n.Files, file)
	}

	return n, nil
}

func (n *NZB) Name() string {
	if name, ok := n.Meta["name"]; ok {
		return name
	}
	if title, ok := n.Meta["title"]; ok {
		return title
	}
	return ""
}

func (n *NZB) TotalSize() int64 {
	var total int64
	for _, f := range n.Files {
		for _, s := range f.Segments {
			total += s.Bytes
		}
	}
	return total
}
