package zcredis

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/driftdev/zencache"
	"github.com/driftdev/zencache/zcerror"
	"github.com/redis/go-redis/v9"
	"time"
)

type RedisClient interface {
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Get(ctx context.Context, key string) *redis.StringCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
}

var _ zencache.CacheProvider = (*Backend)(nil)

type Backend struct {
	client RedisClient
}

func NewBackend(client RedisClient) *Backend {
	return &Backend{client: client}
}

func (b *Backend) Set(ctx context.Context, key string, data any, expiry time.Duration) error {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	_, err = b.client.Set(ctx, key, dataBytes, expiry).Result()
	if err != nil {
		return err
	}

	return nil
}

func (b *Backend) Get(ctx context.Context, key string, data any) error {
	dataBytes, err := b.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return zcerror.ErrorValueNotFound
	}
	if err != nil {
		return err
	}

	err = json.Unmarshal(dataBytes, &data)
	if err != nil {
		return err
	}

	return nil
}

func (b *Backend) Delete(ctx context.Context, keys ...string) error {
	_, err := b.client.Del(ctx, keys...).Result()
	if err != nil {
		return err
	}

	return nil
}
