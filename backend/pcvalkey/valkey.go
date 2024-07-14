package pcvalkey

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/driftdev/polycache-go"
	"github.com/driftdev/polycache-go/pcerror"
	"github.com/redis/go-redis/v9"
	"time"
)

type ValKeyBackend struct {
	client *redis.Client
}

var _ polycache.IPolyCache = (*ValKeyBackend)(nil)

func NewValKeyBackend(client *redis.Client) *ValKeyBackend {
	return &ValKeyBackend{
		client: client,
	}
}

func (vkb *ValKeyBackend) Set(ctx context.Context, key string, data any, expiry time.Duration) error {
	item := polycache.NewItem(data)
	item.SetExpiration(expiry)

	itemBytes, err := json.Marshal(item)
	if err != nil {
		return err
	}

	_, err = vkb.client.Set(ctx, key, itemBytes, expiry).Result()
	if err != nil {
		return err
	}

	return nil
}

func (vkb *ValKeyBackend) Get(ctx context.Context, key string, data any) error {
	result, err := vkb.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return pcerror.PolyCacheErrorValueNotFound
	}
	if err != nil {
		return err
	}

	var item polycache.Item
	err = json.Unmarshal(result, &item)
	if err != nil {
		return err
	}

	if item.IsExpired() {
		err := vkb.Delete(ctx, key)
		if err != nil {
			return err
		}
		return pcerror.PolyCacheErrorValueNotFound
	}

	err = item.ParseData(&data)
	if err != nil {
		return err
	}

	return nil
}

func (vkb *ValKeyBackend) Delete(ctx context.Context, key string) error {
	_, err := vkb.client.Del(ctx, key).Result()
	if err != nil {
		return err
	}

	return nil
}
