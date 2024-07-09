package polycache

import (
	"context"
	"time"
)

type IPolyCache interface {
	Set(ctx context.Context, key string, value string, expiry time.Duration) error
	Get(ctx context.Context, key string) (string, error)

	Delete(ctx context.Context, key string) error
}

type PolyCache struct {
	provider IPolyCache
}

func NewCache(provider IPolyCache) *PolyCache {
	return &PolyCache{provider: provider}
}

func (p *PolyCache) Set(ctx context.Context, key string, value string, expiry time.Duration) error {
	return p.provider.Set(ctx, key, value, expiry)
}

func (p *PolyCache) Get(ctx context.Context, key string) (string, error) {
	return p.provider.Get(ctx, key)
}

func (p *PolyCache) Delete(ctx context.Context, key string) error {
	return p.provider.Delete(ctx, key)
}
