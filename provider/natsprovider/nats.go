package natsprovider

import (
	"context"
	"github.com/driftdev/polycache-go"
	"github.com/driftdev/polycache-go/pcerror"
	"github.com/nats-io/nats.go"
	"time"
)

type NATSProvider struct {
	client nats.KeyValue
}

func New(client nats.KeyValue) polycache.IPolyCache {
	return &NATSProvider{
		client: client,
	}
}

func (np *NATSProvider) Set(ctx context.Context, key string, value string, expiry time.Duration) error {
	_, err := np.client.PutString(key, value)
	if err != nil {
		return err
	}

	return nil
}

func (np *NATSProvider) Get(ctx context.Context, key string) (string, error) {
	result, err := np.client.Get(key)
	if err != nil {
		return "", err
	}

	if result == nil {
		return "", pcerror.PolyCacheErrorValueNotFound
	}

	return string(result.Value()), nil
}

func (np *NATSProvider) Delete(ctx context.Context, key string) error {
	err := np.client.Purge(key)
	if err != nil {
		return err
	}

	return nil
}
