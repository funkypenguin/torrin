package nzb

import (
	"testing"
)

func TestTotalSize_EnforcePlanLimits(t *testing.T) {
	tests := []struct {
		name      string
		segments  []Segment
		maxBytes  int64
		shouldFit bool
	}{
		{
			name: "small NZB under free plan 10GB",
			segments: []Segment{
				{MessageID: "a@t", Bytes: 500000},
				{MessageID: "b@t", Bytes: 500000},
			},
			maxBytes:  10_000_000_000, // 10GB
			shouldFit: true,
		},
		{
			name: "large NZB over free plan 10GB",
			segments: func() []Segment {
				// 15000 segments of 1MB each = 15GB
				segs := make([]Segment, 15000)
				for i := range segs {
					segs[i] = Segment{MessageID: "x@t", Bytes: 1024 * 1024}
				}
				return segs
			}(),
			maxBytes:  10_000_000_000, // 10GB
			shouldFit: false,
		},
		{
			name: "25GB NZB fits starter plan",
			segments: func() []Segment {
				// 20000 segments of 1MB = 20GB
				segs := make([]Segment, 20000)
				for i := range segs {
					segs[i] = Segment{MessageID: "x@t", Bytes: 1024 * 1024}
				}
				return segs
			}(),
			maxBytes:  25_000_000_000, // 25GB
			shouldFit: true,
		},
		{
			name: "25GB NZB exceeds free plan",
			segments: func() []Segment {
				segs := make([]Segment, 25000)
				for i := range segs {
					segs[i] = Segment{MessageID: "x@t", Bytes: 1024 * 1024}
				}
				return segs
			}(),
			maxBytes:  10_000_000_000, // 10GB
			shouldFit: false,
		},
		{
			name:      "zero max means no limit",
			segments:  []Segment{{MessageID: "a@t", Bytes: 999999999999}},
			maxBytes:  0,
			shouldFit: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &NZB{Files: []File{{Segments: tt.segments}}}
			size := n.TotalSize()
			fits := tt.maxBytes == 0 || size <= tt.maxBytes
			if fits != tt.shouldFit {
				t.Errorf("size=%d maxBytes=%d: shouldFit=%v got=%v", size, tt.maxBytes, tt.shouldFit, fits)
			}
		})
	}
}

func TestTotalSize_MultipleFiles(t *testing.T) {
	n := &NZB{
		Files: []File{
			{Segments: []Segment{
				{MessageID: "a@t", Bytes: 1000},
				{MessageID: "b@t", Bytes: 2000},
			}},
			{Segments: []Segment{
				{MessageID: "c@t", Bytes: 3000},
			}},
		},
	}
	if n.TotalSize() != 6000 {
		t.Fatalf("expected 6000, got %d", n.TotalSize())
	}
}

func TestTotalSize_Empty(t *testing.T) {
	n := &NZB{}
	if n.TotalSize() != 0 {
		t.Fatalf("expected 0, got %d", n.TotalSize())
	}
}
