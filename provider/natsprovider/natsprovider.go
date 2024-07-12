package natsprovider

import (
	"context"
	"encoding/json"
	"github.com/driftdev/polycache-go"
	"github.com/driftdev/polycache-go/pcerror"
	"github.com/nats-io/nats.go"
	"time"
)

type data struct {
	Value      string    `json:"value"`
	Expiration time.Time `json:"expiration"`
}

type NATSProvider struct {
	client nats.KeyValue
}

var _ polycache.IPolyCache = (*NATSProvider)(nil)

func New(client nats.KeyValue) *NATSProvider {
	return &NATSProvider{
		client: client,
	}
}

func (np *NATSProvider) Set(ctx context.Context, key string, data any, expiry time.Duration) error {
	item := polycache.NewItem(data)
	item.SetExpiration(expiry)

	itemBytes, err := json.Marshal(item)
	if err != nil {
		return err
	}

	_, err = np.client.Put(key, itemBytes)
	if err != nil {
		return err
	}

	return nil
}

func (np *NATSProvider) Get(ctx context.Context, key string, data any) error {
	result, err := np.client.Get(key)
	if err != nil {
		return err
	}
	if result == nil {
		return pcerror.PolyCacheErrorValueNotFound
	}

	var item polycache.Item
	err = json.Unmarshal(result.Value(), &item)
	if err != nil {
		return err
	}

	if item.IsExpired() {
		err := np.Delete(ctx, key)
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

func (np *NATSProvider) Delete(ctx context.Context, key string) error {
	err := np.client.Purge(key)
	if err != nil {
		return err
	}

	return nil
}
