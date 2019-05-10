package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/oauth2"
	googleoauth2 "golang.org/x/oauth2/google"
)

// FindDefaultCredentialsWithProactiveCache is a wrapper of golang.org/x/oauth2/google.FindDefaultCredentials.
// It runs goroutine which periodically fetches new token and caches it in background.
// The goroutine keeps running until the ctx ends.
//
// FindDefaultCredentials searches for "Application Default Credentials".
//
// It looks for credentials in the following places,
// preferring the first location found:
//
//   1. A JSON file whose path is specified by the
//      GOOGLE_APPLICATION_CREDENTIALS environment variable.
//   2. A JSON file in a location known to the gcloud command-line tool.
//      On Windows, this is %APPDATA%/gcloud/application_default_credentials.json.
//      On other systems, $HOME/.config/gcloud/application_default_credentials.json.
//   3. On Google App Engine standard first generation runtimes (<= Go 1.9) it uses
//      the appengine.AccessToken function.
//   4. On Google Compute Engine, Google App Engine standard second generation runtimes
//      (>= Go 1.11), and Google App Engine flexible environment, it fetches
//      credentials from the metadata server.
//      (In this final case any provided scopes are ignored.)
func FindDefaultCredentialsWithProactiveCache(ctx context.Context, scopes ...string) (*googleoauth2.Credentials, error) {
	ts := &proactiveCacheTokenSource{
		new: &forceNewTokenSource{ctx: ctx, scopes: scopes},
	}

	go ts.run(ctx)

	return &googleoauth2.Credentials{
		TokenSource: ts,
	}, nil
}

// DefaultTokenSourceWithProactiveCache returns the token source for
// "Application Default Credentials".
// It is a shortcut for FindDefaultCredentials(ctx, scope).TokenSource.
func DefaultTokenSourceWithProactiveCache(ctx context.Context, scope ...string) (oauth2.TokenSource, error) {
	creds, err := FindDefaultCredentialsWithProactiveCache(ctx, scope...)
	if err != nil {
		return nil, err
	}
	return creds.TokenSource, nil
}

type forceNewTokenSource struct {
	ctx    context.Context
	scopes []string
}

func (s *forceNewTokenSource) Token() (*oauth2.Token, error) {
	fmt.Println("ForceNewTokenSource.Token()")
	creds, err := googleoauth2.FindDefaultCredentials(s.ctx, s.scopes...)
	if err != nil {
		fmt.Printf("ForceNewTokenSource.Token() err %+v\n", err)
		return nil, err
	}
	return creds.TokenSource.Token()
}

type proactiveCacheTokenSource struct {
	new oauth2.TokenSource

	mu sync.RWMutex
	t  *oauth2.Token
}

func (s *proactiveCacheTokenSource) Token() (*oauth2.Token, error) {
	s.mu.RLock()
	tk := s.t
	s.mu.RUnlock()

	if tk.Valid() {
		return tk, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	fmt.Println("Token().NewToken")
	t, err := s.new.Token()
	if err != nil {
		fmt.Printf("Token().NewToken err %+v\n", err)
		return nil, err
	}
	s.t = t
	fmt.Println("Token().Finish")
	return t, nil
}

func (s *proactiveCacheTokenSource) run(ctx context.Context) {
	maxWait := 10 * time.Second
	initialWait := 100 * time.Millisecond

	ticker := time.NewTicker(38 * time.Minute)
	defer ticker.Stop()

	retrych := make(chan struct{}, 1)
	retrych <- struct{}{}
	wait := initialWait

	for {
		select {
		case <-ctx.Done():
			fmt.Println("proactiveCacheTokenSource.run() ctx.Done()")
			return
		case <-ticker.C:
		case <-retrych:
		}

		fmt.Println("proactiveCacheTokenSource.newToken()")
		token, err := s.new.Token()
		if err != nil {
			fmt.Printf("proactiveCacheTokenSource.newToken().error %+v\n", err)
			time.Sleep(wait)
			wait := wait * 2
			fmt.Printf("proactiveCacheTokenSource.newToken().error.wait %+v\n", wait)
			if wait >= maxWait {
				wait = maxWait
			}

			select {
			case retrych <- struct{}{}:
			default: // non blocking
			}
			continue
		}

		s.mu.Lock()
		s.t = token
		s.mu.Unlock()
		wait = initialWait
	}
}
