package valkeyprovider

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/driftdev/polycache-go"
	"github.com/driftdev/polycache-go/pcerror"
	"github.com/redis/go-redis/v9"
	"time"
)

type ValKeyProvider struct {
	client *redis.Client
}

var _ polycache.IPolyCache = (*ValKeyProvider)(nil)

func New(client *redis.Client) *ValKeyProvider {
	return &ValKeyProvider{
		client: client,
	}
}

func (vkp *ValKeyProvider) Set(ctx context.Context, key string, data any, expiry time.Duration) error {
	item := polycache.NewItem(data)
	item.SetExpiration(expiry)

	itemBytes, err := json.Marshal(item)
	if err != nil {
		return err
	}

	_, err = vkp.client.Set(ctx, key, itemBytes, expiry).Result()
	if err != nil {
		return err
	}

	return nil
}

func (vkp *ValKeyProvider) Get(ctx context.Context, key string, data any) error {
	result, err := vkp.client.Get(ctx, key).Bytes()
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
		err := vkp.Delete(ctx, key)
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

func (vkp *ValKeyProvider) Delete(ctx context.Context, key string) error {
	_, err := vkp.client.Del(ctx, key).Result()
	if err != nil {
		return err
	}

	return nil
}
