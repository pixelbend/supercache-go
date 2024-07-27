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

type Backend struct {
	client *redis.Client
}

var _ zencache.IZenCache = (*Backend)(nil)

func NewBackend(client *redis.Client) *Backend {
	return &Backend{
		client: client,
	}
}

func (b *Backend) Set(ctx context.Context, key string, data any, expiry time.Duration) error {
	item := zencache.NewItem(data)
	item.SetExpiration(expiry)

	itemBytes, err := json.Marshal(item)
	if err != nil {
		return err
	}

	_, err = b.client.Set(ctx, key, itemBytes, expiry).Result()
	if err != nil {
		return err
	}

	return nil
}

func (b *Backend) Get(ctx context.Context, key string, data any) error {
	result, err := b.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return zcerror.ErrorValueNotFound
	}
	if err != nil {
		return err
	}

	var item zencache.Item
	err = json.Unmarshal(result, &item)
	if err != nil {
		return err
	}

	if item.IsExpired() {
		go func() {
			_ = b.Delete(ctx, key)
		}()
		return zcerror.ErrorValueNotFound
	}

	err = item.ParseData(&data)
	if err != nil {
		return err
	}

	return nil
}

func (b *Backend) Delete(ctx context.Context, key string) error {
	_, err := b.client.Del(ctx, key).Result()
	if err != nil {
		return err
	}

	return nil
}
