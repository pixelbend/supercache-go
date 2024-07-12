package bigcacheprovider

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/allegro/bigcache/v3"
	"github.com/driftdev/polycache-go"
	"github.com/driftdev/polycache-go/pcerror"
	"time"
)

type BigCacheProvider struct {
	client *bigcache.BigCache
}

var _ polycache.IPolyCache = (*BigCacheProvider)(nil)

func New(client *bigcache.BigCache) polycache.IPolyCache {
	return &BigCacheProvider{
		client: client,
	}
}

func (bcp *BigCacheProvider) Set(ctx context.Context, key string, data any, expiry time.Duration) error {
	item := polycache.NewItem(data)
	item.SetExpiration(expiry)

	itemBytes, err := json.Marshal(item)
	if err != nil {
		return err
	}

	err = bcp.client.Set(key, itemBytes)
	if err != nil {
		return err
	}

	return nil
}

func (bcp *BigCacheProvider) Get(ctx context.Context, key string, data any) error {
	result, err := bcp.client.Get(key)
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
		err := bcp.Delete(ctx, key)
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

func (bcp *BigCacheProvider) Delete(ctx context.Context, key string) error {
	err := bcp.client.Delete(key)
	if err != nil {
		return err
	}

	return nil
}
