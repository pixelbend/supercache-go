package valkeyprovider

import (
	"context"
	"errors"
	"github.com/driftdev/polycache-go"
	"github.com/driftdev/polycache-go/pcerror"
	"github.com/redis/go-redis/v9"
	"time"
)

type ValKeyProvider struct {
	client *redis.Client
}

var _ polycache.IPolyCache = (*ValKeyProvider)(nil)

func New(client *redis.Client) *ValKeyProvider {
	return &ValKeyProvider{
		client: client,
	}
}

func (vkp *ValKeyProvider) Set(ctx context.Context, key string, value string, expiry time.Duration) error {
	_, err := vkp.client.Set(ctx, key, value, expiry).Result()
	if err != nil {
		return err
	}

	return nil
}

func (vkp *ValKeyProvider) Get(ctx context.Context, key string) (string, error) {
	result, err := vkp.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", pcerror.PolyCacheErrorValueNotFound
	}

	if err != nil {
		return "", err
	}

	return result, nil
}

func (vkp *ValKeyProvider) Delete(ctx context.Context, key string) error {
	_, err := vkp.client.Del(ctx, key).Result()
	if err != nil {
		return err
	}

	return nil
}
