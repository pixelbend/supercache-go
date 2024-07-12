package bigcacheprovider

import (
	"context"
	"encoding/json"
	"github.com/allegro/bigcache/v3"
	"github.com/driftdev/polycache-go"
	"github.com/driftdev/polycache-go/pcerror"
	"time"
)

type data struct {
	Value      string    `json:"value"`
	Expiration time.Time `json:"expiration"`
}

type BigCacheProvider struct {
	client *bigcache.BigCache
}

var _ polycache.IPolyCache = (*BigCacheProvider)(nil)

func New(client *bigcache.BigCache) polycache.IPolyCache {
	return &BigCacheProvider{
		client: client,
	}
}

func (bcp *BigCacheProvider) Set(ctx context.Context, key string, value string, expiry time.Duration) error {
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

	err = bcp.client.Set(key, dataBytes)
	if err != nil {
		return err
	}

	return nil
}

func (bcp *BigCacheProvider) Get(ctx context.Context, key string) (string, error) {
	result, err := bcp.client.Get(key)
	if err != nil {
		return "", err
	}

	var resultData data
	err = json.Unmarshal(result, &resultData)
	if err != nil {
		return "", err
	}

	if !resultData.Expiration.IsZero() && time.Now().After(resultData.Expiration) {
		err := bcp.client.Delete(key)
		if err != nil {
			return "", err
		}
		return "", pcerror.PolyCacheErrorValueNotFound
	}

	return resultData.Value, nil
}

func (bcp *BigCacheProvider) Delete(ctx context.Context, key string) error {
	err := bcp.client.Delete(key)
	if err != nil {
		return err
	}

	return nil
}
