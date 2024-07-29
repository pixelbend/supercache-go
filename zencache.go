package zencache

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/driftdev/zencache/zcerror"
	"github.com/redis/go-redis/v9"
	"time"
)

type RedisClient interface {
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Get(ctx context.Context, key string) *redis.StringCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
}

type ZenCache struct {
	client RedisClient
}

func NewCache(client RedisClient) *ZenCache {
	return &ZenCache{client: client}
}

type Backend struct {
	client *redis.Client
}

func (zc *ZenCache) Set(ctx context.Context, key string, data any, expiry time.Duration) error {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	_, err = zc.client.Set(ctx, key, dataBytes, expiry).Result()
	if err != nil {
		return err
	}

	return nil
}

func (zc *ZenCache) Get(ctx context.Context, key string, data any) error {
	dataBytes, err := zc.client.Get(ctx, key).Bytes()
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

func (zc *ZenCache) Delete(ctx context.Context, keys ...string) error {
	_, err := zc.client.Del(ctx, keys...).Result()
	if err != nil {
		return err
	}

	return nil
}
