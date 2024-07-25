package ocbigcache

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/allegro/bigcache/v3"
	"github.com/driftdev/omnicache"
	"github.com/driftdev/omnicache/ocerror"
	"time"
)

type Backend struct {
	client *bigcache.BigCache
}

var _ omnicache.IOmniCache = (*Backend)(nil)

func NewBackend(client *bigcache.BigCache) *Backend {
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

	err = b.client.Set(key, itemBytes)
	if err != nil {
		return err
	}

	return nil
}

func (b *Backend) Get(ctx context.Context, key string, data any) error {
	result, err := b.client.Get(key)
	if errors.Is(err, bigcache.ErrEntryNotFound) {
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
	err := b.client.Delete(key)
	if err != nil {
		return err
	}

	return nil
}
