package ocvalkey

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/driftdev/omnicache"
	"github.com/driftdev/omnicache/ocerror"
	"github.com/redis/go-redis/v9"
	"time"
)

type Backend struct {
	client *redis.Client
}

var _ omnicache.IOmniCache = (*Backend)(nil)

func NewBackend(client *redis.Client) *Backend {
	return &Backend{
		client: client,
	}
}

func (b *Backend) Set(ctx context.Context, key string, data any, expiry time.Duration) error {
	item := omnicache.NewItem(data)
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
		return ocerror.ErrorValueNotFound
	}
	if err != nil {
		return err
	}

	var item omnicache.Item
	err = json.Unmarshal(result, &item)
	if err != nil {
		return err
	}

	if item.IsExpired() {
		err := b.Delete(ctx, key)
		if err != nil {
			return err
		}
		return ocerror.ErrorValueNotFound
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
