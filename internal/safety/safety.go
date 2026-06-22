// Package safety screens content names/links for material that must never enter
// the cache, blocking it at every ingest path (torrents, hosters, usenet,
// telegram). The match logic lives here; the term list does NOT ;) Terms are loaded
// at runtime via SetTerms from private storage, so no sensitive word list is ever
// committed to this public repo and the list can be tuned without a redeploy.
package safety

import (
	"strings"
	"sync/atomic"
)

type Verdict struct {
	Blocked bool
	Ban     bool
	Reason  string
}

type termSet struct {
	hard []string
	soft []string
}

var terms atomic.Pointer[termSet]

func SetTerms(hard, soft []string) {
	terms.Store(&termSet{hard: lower(hard), soft: lower(soft)})
}

func lower(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.ToLower(strings.TrimSpace(s))
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func Screen(texts ...string) Verdict {
	ts := terms.Load()
	var soft Verdict
	for _, t := range texts {
		l := strings.ToLower(t)
		if strings.Contains(l, ".onion") {
			return Verdict{Blocked: true, Ban: true, Reason: "blocked:onion"}
		}
		if ts == nil {
			continue
		}
		for _, tok := range ts.hard {
			if strings.Contains(l, tok) {
				return Verdict{Blocked: true, Ban: true, Reason: "blocked:term"}
			}
		}
		if !soft.Blocked {
			for _, tok := range ts.soft {
				if strings.Contains(l, tok) {
					soft = Verdict{Blocked: true, Ban: false, Reason: "flagged:term"}
				}
			}
		}
	}
	return soft
}
