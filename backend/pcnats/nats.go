package pcnats

import (
	"context"
	"encoding/json"
	"github.com/driftdev/polycache-go"
	"github.com/driftdev/polycache-go/pcerror"
	"github.com/nats-io/nats.go"
	"time"
)

type NATSBackend struct {
	client nats.KeyValue
}

var _ polycache.IPolyCache = (*NATSBackend)(nil)

func NewNATSBackend(client nats.KeyValue) *NATSBackend {
	return &NATSBackend{
		client: client,
	}
}

func (nb *NATSBackend) Set(ctx context.Context, key string, data any, expiry time.Duration) error {
	item := polycache.NewItem(data)
	item.SetExpiration(expiry)

	itemBytes, err := json.Marshal(item)
	if err != nil {
		return err
	}

	_, err = nb.client.Put(key, itemBytes)
	if err != nil {
		return err
	}

	return nil
}

func (nb *NATSBackend) Get(ctx context.Context, key string, data any) error {
	result, err := nb.client.Get(key)
	if err != nil {
		return err
	}
	if result == nil {
		return pcerror.ErrorValueNotFound
	}

	var item polycache.Item
	err = json.Unmarshal(result.Value(), &item)
	if err != nil {
		return err
	}

	if item.IsExpired() {
		err := nb.Delete(ctx, key)
		if err != nil {
			return err
		}
		return pcerror.ErrorValueNotFound
	}

	err = item.ParseData(&data)
	if err != nil {
		return err
	}

	return nil
}

func (nb *NATSBackend) Delete(ctx context.Context, key string) error {
	err := nb.client.Purge(key)
	if err != nil {
		return err
	}

	return nil
}
