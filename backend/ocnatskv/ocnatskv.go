package ocnatskv

import (
	"context"
	"encoding/json"
	"github.com/driftdev/omnicache"
	"github.com/driftdev/omnicache/ocerror"
	"github.com/nats-io/nats.go"
	"time"
)

type Backend struct {
	client nats.KeyValue
}

var _ omnicache.IOmniCache = (*Backend)(nil)

func NewBackend(client nats.KeyValue) *Backend {
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

	_, err = b.client.Put(key, itemBytes)
	if err != nil {
		return err
	}

	return nil
}

func (b *Backend) Get(ctx context.Context, key string, data any) error {
	result, err := b.client.Get(key)
	if err != nil {
		return err
	}
	if result == nil {
		return ocerror.ErrorValueNotFound
	}

	var item omnicache.Item
	err = json.Unmarshal(result.Value(), &item)
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
	err := b.client.Purge(key)
	if err != nil {
		return err
	}

	return nil
}
