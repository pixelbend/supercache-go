package zencache

import (
	"context"
	"time"
)

type CacheProvider interface {
	Set(ctx context.Context, key string, data any, expiry time.Duration) error
	Get(ctx context.Context, key string, data any) error
	Delete(ctx context.Context, keys ...string) error
}

type Cache struct {
	cache CacheProvider
}

func NewCache(cache CacheProvider) *Cache {
	return &Cache{cache: cache}
}

func (c *Cache) Set(ctx context.Context, key string, data any, expiry time.Duration) error {
	return c.cache.Set(ctx, key, data, expiry)
}

func (c *Cache) Get(ctx context.Context, key string, data any) error {
	return c.cache.Get(ctx, key, data)
}

func (c *Cache) Delete(ctx context.Context, keys ...string) error {
	return c.cache.Delete(ctx, keys...)
}
