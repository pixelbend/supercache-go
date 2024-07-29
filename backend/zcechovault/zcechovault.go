package zcechovault

import (
	"context"
	"encoding/json"
	"github.com/driftdev/zencache"
	"github.com/driftdev/zencache/zcerror"
	"github.com/echovault/echovault/echovault"
	"time"
)

type EchoVaultClient interface {
	Set(key string, value string, options echovault.SetOptions) (string, bool, error)
	Get(key string) (string, error)
	Del(keys ...string) (int, error)
}

var _ zencache.CacheProvider = (*Backend)(nil)

type Backend struct {
	client EchoVaultClient
}

func NewBackend(client *echovault.EchoVault) *Backend {
	return &Backend{client: client}
}

func (b *Backend) Set(ctx context.Context, key string, data any, expiry time.Duration) error {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	_, _, err = b.client.Set(key, string(dataBytes), echovault.SetOptions{
		EX: int(expiry.Seconds()),
	})
	if err != nil {
		return err
	}

	return nil
}

func (b *Backend) Get(ctx context.Context, key string, data any) error {
	dataString, err := b.client.Get(key)
	if dataString == "" {
		return zcerror.ErrorValueNotFound
	}
	if err != nil {
		return err
	}

	err = json.Unmarshal([]byte(dataString), &data)
	if err != nil {
		return err
	}

	return nil
}

func (b *Backend) Delete(ctx context.Context, keys ...string) error {
	_, err := b.client.Del(keys...)
	if err != nil {
		return err
	}

	return nil
}
