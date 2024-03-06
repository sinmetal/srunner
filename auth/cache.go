package auth

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/oauth2"
)

type ProactiveCacheTokenSource struct {
	new oauth2.TokenSource

	mu sync.RWMutex
	t  *oauth2.Token

	renewPeriod time.Duration
	maxWait     time.Duration
	initialWait time.Duration
}

type Config struct {
	RenewPeriod time.Duration
	MaxWait     time.Duration
	InitialWait time.Duration
}

func NewProactiveCacheTokenSource(ts oauth2.TokenSource, cfg Config) (*ProactiveCacheTokenSource, error) {
	if cfg.RenewPeriod == 0 {
		return nil, errors.New("renew period must be greater than zero")
	}
	if cfg.MaxWait == 0 {
		cfg.MaxWait = 10 * time.Second
	}
	if cfg.InitialWait == 0 {
		cfg.InitialWait = 100 * time.Millisecond
	}
	return &ProactiveCacheTokenSource{
		new:         ts,
		renewPeriod: cfg.RenewPeriod,
		maxWait:     cfg.MaxWait,
		initialWait: cfg.InitialWait,
	}, nil
}

func (s *ProactiveCacheTokenSource) Token() (*oauth2.Token, error) {
	s.mu.RLock()
	tk := s.t
	s.mu.RUnlock()

	if tk.Valid() {
		return tk, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	t, err := s.new.Token()
	if err != nil {
		return nil, fmt.Errorf("failed oauth2 new.Token() : %w", err)
	}
	s.t = t
	return t, nil
}

func (s *ProactiveCacheTokenSource) Run(ctx context.Context) {
	ticker := time.NewTicker(s.renewPeriod)
	defer ticker.Stop()

	retrych := make(chan struct{}, 1)
	retrych <- struct{}{}
	wait := s.initialWait

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-retrych:
		}

		token, err := s.new.Token()
		if err != nil {
			fmt.Printf("failed oauth2.new.Token(). retry wait %s err=%s\n", wait, err)
			time.Sleep(wait)
			wait = wait * 2
			if wait >= s.maxWait {
				wait = s.maxWait
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
		wait = s.initialWait
	}
}
