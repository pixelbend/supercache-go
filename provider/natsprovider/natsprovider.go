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

func New(client nats.KeyValue) polycache.IPolyCache {
	return &NATSProvider{
		client: client,
	}
}

func (np *NATSProvider) Set(ctx context.Context, key string, value string, expiry time.Duration) error {
	var expiration time.Time
	if expiry != 0 {
		expiration = time.Now().Add(expiry)
	}

	d := data{
		Value:      value,
		Expiration: expiration,
	}

	dataBytes, err := json.Marshal(d)
	if err != nil {
		return err
	}

	_, err = np.client.Put(key, dataBytes)
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

	var resultData data
	err = json.Unmarshal(result.Value(), &resultData)
	if err != nil {
		return "", err
	}

	if !resultData.Expiration.IsZero() && time.Now().After(resultData.Expiration) {
		err := np.client.Delete(key)
		if err != nil {
			return "", err
		}
		return "", pcerror.PolyCacheErrorValueNotFound
	}

	return resultData.Value, nil
}

func (np *NATSProvider) Delete(ctx context.Context, key string) error {
	err := np.client.Purge(key)
	if err != nil {
		return err
	}

	return nil
}
