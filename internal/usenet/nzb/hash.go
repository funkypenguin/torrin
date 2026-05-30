package nzb

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

func Hash(n *NZB) string {
	var ids []string
	for _, f := range n.Files {
		for _, s := range f.Segments {
			ids = append(ids, s.MessageID)
		}
	}
	sort.Strings(ids)

	h := sha256.New()
	for _, id := range ids {
		h.Write([]byte(id))
	}

	full := h.Sum(nil)
	return hex.EncodeToString(full[:20])
}
