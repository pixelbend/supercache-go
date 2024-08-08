package zencache

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/redis/go-redis/v9"
	"time"
)

type RedisClient interface {
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Get(ctx context.Context, key string) *redis.StringCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
}

type Cache struct {
	client RedisClient
}

func NewCache(cache RedisClient) *Cache {
	return &Cache{client: cache}
}

func (c *Cache) Set(ctx context.Context, key string, data any, expiry time.Duration) error {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	_, err = c.client.Set(ctx, key, dataBytes, expiry).Result()
	if err != nil {
		return err
	}

	return nil
}

func (c *Cache) Get(ctx context.Context, key string, data any) (bool, error) {
	dataBytes, err := c.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	err = json.Unmarshal(dataBytes, &data)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (c *Cache) Delete(ctx context.Context, keys ...string) error {
	_, err := c.client.Del(ctx, keys...).Result()
	if err != nil {
		return err
	}

	return nil
}
