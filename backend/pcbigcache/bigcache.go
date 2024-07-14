package pcbigcache

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/allegro/bigcache/v3"
	"github.com/driftdev/polycache-go"
	"github.com/driftdev/polycache-go/pcerror"
	"time"
)

type BigCacheBackend struct {
	client *bigcache.BigCache
}

var _ polycache.IPolyCache = (*BigCacheBackend)(nil)

func NewBigCacheBackend(client *bigcache.BigCache) polycache.IPolyCache {
	return &BigCacheBackend{
		client: client,
	}
}

func (bcb *BigCacheBackend) Set(ctx context.Context, key string, data any, expiry time.Duration) error {
	item := polycache.NewItem(data)
	item.SetExpiration(expiry)

	itemBytes, err := json.Marshal(item)
	if err != nil {
		return err
	}

	err = bcb.client.Set(key, itemBytes)
	if err != nil {
		return err
	}

	return nil
}

func (bcb *BigCacheBackend) Get(ctx context.Context, key string, data any) error {
	result, err := bcb.client.Get(key)
	if errors.Is(err, bigcache.ErrEntryNotFound) {
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
		err := bcb.Delete(ctx, key)
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

func (bcb *BigCacheBackend) Delete(ctx context.Context, key string) error {
	err := bcb.client.Delete(key)
	if err != nil {
		return err
	}

	return nil
}
