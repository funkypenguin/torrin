package usenet

import (
	"context"

	"github.com/torrin-app/torrin/internal/usenet/nntp"
)

type CredentialProvider interface {
	GetUsenetCredentials(ctx context.Context, userID string) (*nntp.Credentials, error)
}
