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

func New(client *redis.Client) polycache.IPolyCache {
	return &ValKeyProvider{
		client: client,
	}
}

func (vkp *ValKeyProvider) Set(ctx context.Context, key string, value string, expiry *time.Duration) error {
	exp := time.Duration(0)
	
	if expiry != nil {
		exp = *expiry
	}

	_, err := vkp.client.Set(ctx, key, value, exp).Result()
	if err != nil {
		return err
	}

	return nil
}

func (vkp *ValKeyProvider) Get(ctx context.Context, key string) (string, error) {
	result, err := vkp.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", pcerror.PolyCacheErrorValueNotFound
		}
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
