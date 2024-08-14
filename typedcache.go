package resilicache

import (
	"context"
	"encoding/json"
	"time"
)

type TypedCache[T any] struct {
	cache *Cache
}

func NewTypedCache[T any](cache *Cache) *TypedCache[T] {
	return &TypedCache[T]{
		cache: cache,
	}
}

func (tc *TypedCache[T]) Set(ctx context.Context, key string, value T, expire time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return tc.cache.Set(ctx, key, data, expire)
}

func (tc *TypedCache[T]) Get(ctx context.Context, key string) (T, error) {
	var value T

	dataBytes, err := tc.cache.Get(ctx, key)
	if err != nil {
		return value, err
	}

	if dataBytes == nil {
		return value, nil
	}

	err = json.Unmarshal(dataBytes, &value)
	return value, err
}

func (tc *TypedCache[T]) Delete(ctx context.Context, key string) error {
	return tc.cache.Delete(ctx, key)
}

func (tc *TypedCache[T]) FetchSingle(ctx context.Context, key string, expire time.Duration, fn func() (T, error)) (T, error) {
	var value T

	fetchSingleFn := func() ([]byte, error) {
		result, err := fn()
		if err != nil {
			return nil, err
		}
		return json.Marshal(result)
	}

	dataBytes, err := tc.cache.FetchSingle(ctx, key, expire, fetchSingleFn)
	if err != nil {
		return value, err
	}

	err = json.Unmarshal(dataBytes, &value)
	return value, err
}

func (tc *TypedCache[T]) FetchBatch(ctx context.Context, keys []string, expire time.Duration, fn func(indexes []int) (map[int]T, error)) (map[int]T, error) {
	fetchBatchFn := func(indexes []int) (map[int][]byte, error) {
		result, err := fn(indexes)
		if err != nil {
			return nil, err
		}

		byteResult := make(map[int][]byte)
		for idx, val := range result {
			data, err := json.Marshal(val)
			if err != nil {
				return nil, err
			}
			byteResult[idx] = data
		}
		return byteResult, nil
	}

	dataBytes, err := tc.cache.FetchBatch(ctx, keys, expire, fetchBatchFn)
	if err != nil {
		return nil, err
	}

	values := make(map[int]T)
	for idx, data := range dataBytes {
		var value T
		if err := json.Unmarshal(data, &value); err != nil {
			continue
		}
		values[idx] = value
	}

	return values, nil
}

func (tc *TypedCache[T]) TagAsDeletedSingle(ctx context.Context, key string) error {
	return tc.cache.TagAsDeletedSingle(ctx, key)
}

func (tc *TypedCache[T]) TagAsDeletedBatch(ctx context.Context, keys []string) error {
	return tc.cache.TagAsDeletedBatch(ctx, keys)
}

func (tc *TypedCache[T]) RawGet(ctx context.Context, key string) (T, error) {
	var value T

	data, err := tc.cache.RawGet(ctx, key)
	if err != nil {
		return value, err
	}

	err = json.Unmarshal(data, &value)
	return value, err
}

func (tc *TypedCache[T]) RawSet(ctx context.Context, key string, value T, expire time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return tc.cache.RawSet(ctx, key, data, expire)
}

func (tc *TypedCache[T]) LockForUpdate(ctx context.Context, key string, owner string) error {
	return tc.cache.LockForUpdate(ctx, key, owner)
}

func (tc *TypedCache[T]) UnlockForUpdate(ctx context.Context, key string, owner string) error {
	return tc.cache.UnlockForUpdate(ctx, key, owner)
}
