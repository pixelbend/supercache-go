package omnicache

import (
	"context"
	"time"
)

type IOmniCache interface {
	Set(ctx context.Context, key string, data any, expiry time.Duration) error
	Get(ctx context.Context, key string, data any) error
	Delete(ctx context.Context, key string) error
}

type OmniCache struct {
	provider IOmniCache
}

func NewCache(provider IOmniCache) *OmniCache {
	return &OmniCache{provider: provider}
}

func (p *OmniCache) Set(ctx context.Context, key string, data any, expiry time.Duration) error {
	return p.provider.Set(ctx, key, data, expiry)
}

func (p *OmniCache) Get(ctx context.Context, key string, data any) error {
	return p.provider.Get(ctx, key, data)
}

func (p *OmniCache) Delete(ctx context.Context, key string) error {
	return p.provider.Delete(ctx, key)
}
