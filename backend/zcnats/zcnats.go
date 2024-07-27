package zcnats

import (
	"context"
	"encoding/json"
	"github.com/driftdev/zencache"
	"github.com/driftdev/zencache/zcerror"
	"github.com/nats-io/nats.go"
	"strings"
	"time"
)

type Backend struct {
	client nats.KeyValue
}

var _ zencache.IZenCache = (*Backend)(nil)

func NewBackend(client nats.KeyValue) *Backend {
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

	_, err = b.client.Put(formatKey(key), itemBytes)
	if err != nil {
		return err
	}

	return nil
}

func (b *Backend) Get(ctx context.Context, key string, data any) error {
	result, err := b.client.Get(formatKey(key))
	if err != nil {
		return err
	}
	if result == nil {
		return zcerror.ErrorValueNotFound
	}

	var item zencache.Item
	err = json.Unmarshal(result.Value(), &item)
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
	err := b.client.Purge(formatKey(key))
	if err != nil {
		return err
	}

	return nil
}

func formatKey(key string) string {
	return strings.ReplaceAll(key, ":", ".")
}
