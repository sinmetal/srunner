package auth

import (
	"context"
	"net/http"
	"time"

	"golang.org/x/oauth2"
	googleoauth2 "golang.org/x/oauth2/google"
)

const tokenRenewPeriod = 30 * time.Minute

func newProactiveCacheTokenSource(ctx context.Context, tokenSource oauth2.TokenSource) (oauth2.TokenSource, error) {
	cfg := Config{
		RenewPeriod: tokenRenewPeriod,
	}
	ts, err := NewProactiveCacheTokenSource(tokenSource, cfg)
	if err != nil {
		return nil, err
	}
	go ts.Run(ctx)
	return ts, nil
}

// DefaultTokenSourceWithProactiveCache returns the token source for
// "Application Default Credentials".
// It is a shortcut for FindDefaultCredentials(ctx, scope).TokenSource.
func DefaultTokenSourceWithProactiveCache(ctx context.Context, scopes ...string) (oauth2.TokenSource, error) {
	return newProactiveCacheTokenSource(ctx, &forceNewTokenSource{ctx: ctx, scopes: scopes})
}

// TokenOption provides ability to configure the google TokenSource to issue Google ID Tokens.
type TokenOption func(opt *tokenOption)

type tokenOption struct {
	credentialsJSON []byte
	serviceAccount  string
	mdHTTPClient    *http.Client
}
type forceNewTokenSource struct {
	ctx    context.Context
	scopes []string
}

func (s *forceNewTokenSource) Token() (*oauth2.Token, error) {
	creds, err := googleoauth2.FindDefaultCredentials(s.ctx, s.scopes...)
	if err != nil {
		return nil, err
	}
	return creds.TokenSource.Token()
}
