package realdebrid

import "context"

// KeyProvider returns an RD API key for a given user, if user have one configured.
type KeyProvider interface {
	GetRDKey(ctx context.Context, userID string) (string, error)
}

// HashLookup checks if any synced user has a given info hash in their RD library.
// Returns the API key of the user who has it,or empty string if not found.
type HashLookup interface {
	FindRDKeyForHash(ctx context.Context, infoHash string) (string, error)
}
