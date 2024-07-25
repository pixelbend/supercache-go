package zencache

import (
	"context"
	"time"
)

type IZenCache interface {
	Set(ctx context.Context, key string, data any, expiry time.Duration) error
	Get(ctx context.Context, key string, data any) error
	Delete(ctx context.Context, key string) error
}

type ZenCache struct {
	provider IZenCache
}

func NewCache(provider IZenCache) *ZenCache {
	return &ZenCache{provider: provider}
}

func (p *ZenCache) Set(ctx context.Context, key string, data any, expiry time.Duration) error {
	return p.provider.Set(ctx, key, data, expiry)
}

func (p *ZenCache) Get(ctx context.Context, key string, data any) error {
	return p.provider.Get(ctx, key, data)
}

func (p *ZenCache) Delete(ctx context.Context, key string) error {
	return p.provider.Delete(ctx, key)
}
