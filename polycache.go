package polycache

import (
	"context"
	"time"
)

type IPolyCache interface {
	Set(ctx context.Context, key string, data any, expiry time.Duration) error
	Get(ctx context.Context, key string, data any) error
	Delete(ctx context.Context, key string) error
}

type PolyCache struct {
	provider IPolyCache
}

func NewCache(provider IPolyCache) *PolyCache {
	return &PolyCache{provider: provider}
}

func (p *PolyCache) Set(ctx context.Context, key string, data any, expiry time.Duration) error {
	return p.provider.Set(ctx, key, data, expiry)
}

func (p *PolyCache) Get(ctx context.Context, key string, data any) error {
	return p.provider.Get(ctx, key, data)
}

func (p *PolyCache) Delete(ctx context.Context, key string) error {
	return p.provider.Delete(ctx, key)
}
