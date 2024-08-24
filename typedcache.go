package resilicache

import (
	"context"
	"encoding/json"
	"time"
)

// TypedCache is a generic wrapper around the Cache struct, allowing strongly-typed cache operations.
// It provides a type-safe way to store and retrieve values in the cache by leveraging Go's generics.
//
// This wrapper simplifies the process of storing and retrieving typed values by handling JSON serialization
// and deserialization internally. It delegates the actual caching operations to an underlying Cache instance.
//
// Type Parameters:
//   - T: The type of the values that will be cached.
type TypedCache[T any] struct {
	cache *Cache
}

// NewTypedCache creates a new instance of TypedCache for the specified type `T`.
// It initializes the TypedCache with the provided Cache instance, allowing for type-safe caching operations.
//
// Parameters:
//   - cache: A pointer to the Cache instance that will handle the underlying cache operations.
//
// Type Parameters:
//   - T: The type of the values that will be cached.
//
// Returns:
//   - *TypedCache[T]: A pointer to the newly created TypedCache instance.
//
// This function is typically used to create a TypedCache for a specific type:
// Example:
//
//	cache := NewCache(redisClient, opts)
//	typedCache := NewTypedCache[string](cache)
func NewTypedCache[T any](cache *Cache) *TypedCache[T] {
	return &TypedCache[T]{
		cache: cache,
	}
}

// Set stores a typed value in the cache under the specified key, with an associated expiration time.
// This method serializes the value to JSON before storing.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - key: The unique identifier for the cache entry (e.g., cache key).
//   - value: The typed value to be stored in the cache, which will be serialized to JSON.
//   - expire: The duration after which the cache entry will expire and be automatically deleted.
//
// Returns:
//   - error: If the serialization or cache operation fails, an error is returned. Otherwise, it returns nil.
//
// Example:
// To store a string value in the cache:
//
//	err := Set(ctx, "user_01J4YHWG45SC7VW684TZB2SZ7K", "example_value", 10*time.Minute)
//	if err != nil {
//	    log.Fatalf("Failed to set cache: %v", err)
//	}
func (tc *TypedCache[T]) Set(ctx context.Context, key string, value T, expire time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return tc.cache.Set(ctx, key, data, expire)
}

// Get retrieves a typed value from the cache using the specified key.
// This method deserializes the value to defined type on retrieval.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - key: The unique identifier for the cache entry (e.g., cache key).
//
// Returns:
//   - T: The typed value retrieved from the cache. If the key does not exist, a zero value for the type `T` is returned.
//   - error: If the cache operation or deserialization fails, an error is returned. Otherwise, it returns nil.
//
// Example:
// To retrieve a string value from the cache:
//
//	value, err := Get(ctx, "user_01J4YHWG45SC7VW684TZB2SZ7K")
//	if err != nil {
//	    log.Fatalf("Failed to get cache: %v", err)
//	}
//	log.Printf("Fetched value: %s", value)
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

// Delete removes a typed value from the cache using the specified key.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - key: The unique identifier for the cache entry to be deleted (e.g., cache key).
//
// Returns:
//   - error: If the cache operation fails, an error is returned. Otherwise, it returns nil.
//
// Example:
// To delete a value from the cache:
//
//	err := Delete(ctx, "user_01J4YHWG45SC7VW684TZB2SZ7K")
//	if err != nil {
//	    log.Fatalf("Failed to delete cache key: %v", err)
//	}
func (tc *TypedCache[T]) Delete(ctx context.Context, key string) error {
	return tc.cache.Delete(ctx, key)
}

// FetchSingle retrieves a typed value from the cache using the specified key.
// If the key is not found, the provided function `fn` is called to generate the value, which is then stored in the cache then returned.
// This method deserializes the value to defined type on retrieval.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - key: The unique identifier for the cache entry.
//   - expire: The duration after which the cache entry will expire.
//   - fn: A function that returns the value to be cached if it is not already present.
//
// Returns:
//   - T: The typed value retrieved from or stored in the cache.
//   - error: If the cache operation, serialization, or deserialization fails, an error is returned.
//
// Example:
// To fetch or generate a string value:
//
//	value, err := FetchSingle(ctx, "user_01J4YHWG45SC7VW684TZB2SZ7K", 10*time.Minute, func() (string, error) {
//	    return "generated_value", nil
//	})
//	if err != nil {
//	    log.Fatalf("Failed to fetch cache: %v", err)
//	}
//	log.Printf("Fetched or generated value: %s", value)
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

// TagAsDeletedSingle marks a single cache entry as deleted for the given key.
// This method does not remove the entry immediately but tags it as deleted for delayed deletion.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - key: The unique identifier for the cache entry to be marked as deleted.
//
// Returns:
//   - error: If the cache operation fails, an error is returned.
//
// Example:
// To mark a cache entry as deleted:
//
//	err := TagAsDeletedSingle(ctx, "user_01J4YHWG45SC7VW684TZB2SZ7K")
//	if err != nil {
//	    log.Fatalf("Failed to mark cache key as deleted: %v", err)
//	}
func (tc *TypedCache[T]) TagAsDeletedSingle(ctx context.Context, key string) error {
	return tc.cache.TagAsDeletedSingle(ctx, key)
}

// FetchBatch retrieves multiple typed values from the cache using the specified keys.
// If some keys are not found, the provided function `fn` is called to generate the missing values, which are then stored in the cache then returned.
// This method deserializes the values to defined type on retrieval.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - keys: A slice of unique identifiers for the cache entries.
//   - expire: The duration after which the cache entries will expire.
//   - fn: A function that returns a map of missing index positions to their corresponding values to be cached.
//
// Returns:
//   - map[int]T: A map of index positions to the typed values retrieved from or stored in the cache.
//   - error: If the cache operation, serialization, or deserialization fails, an error is returned.
//
// Example:
// To fetch or generate multiple string values:
//
//	values, err := FetchBatch(ctx, []string{"key1", "key2"}, 10*time.Minute, func(idxs []int) (map[int]string, error) {
//	    return map[int]string{
//	        indexes[0]: "generated_value1",
//	        indexes[1]: "generated_value2",
//	    }, nil
//	})
//	if err != nil {
//	    log.Fatalf("Failed to fetch batch cache: %v", err)
//	}
//	log.Printf("Fetched or generated values: %v", values)
func (tc *TypedCache[T]) FetchBatch(ctx context.Context, keys []string, expire time.Duration, fn func(idxs []int) (map[int]T, error)) (map[int]T, error) {
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

// TagAsDeletedBatch marks multiple cache entries as deleted for the given keys.
// This method does not remove the entries immediately but tags them as deleted for delayed deletion.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - keys: A slice of unique identifiers for the cache entries to be marked as deleted.
//
// Returns:
//   - error: If the cache operation fails, an error is returned.
//
// Example:
// To mark multiple cache entries as deleted:
//
//	err := TagAsDeletedBatch(ctx, []string{"key1", "key2"})
//	if err != nil {
//	    log.Fatalf("Failed to mark cache keys as deleted: %v", err)
//	}
func (tc *TypedCache[T]) TagAsDeletedBatch(ctx context.Context, keys []string) error {
	return tc.cache.TagAsDeletedBatch(ctx, keys)
}

// RawSet stores a typed value in the cache under the specified key, with an associated expiration time.
// This method directly serializes the value to JSON and stores it using the underlying Cache instance's RawSet method.
//
// Warning:
// Use caution when using `RawSet`, as it does not respect any locks that may be in place on the cache entry.
// This could lead to race conditions or overwriting data that is currently being updated by another process.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - key: The unique identifier for the cache entry.
//   - value: The typed value to be stored in the cache, which will be serialized to JSON.
//   - expire: The duration after which the cache entry will expire and be automatically deleted.
//
// Returns:
//   - error: If the serialization or cache operation fails, an error is returned.
//
// Example:
// To directly store a string value in the cache:
//
//	err := RawSet(ctx, "user_01J4YHWG45SC7VW684TZB2SZ7K", "example_value", 10*time.Minute)
//	if err != nil {
//	    log.Fatalf("Failed to set cache: %v", err)
//	}
func (tc *TypedCache[T]) RawSet(ctx context.Context, key string, value T, expire time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return tc.cache.RawSet(ctx, key, data, expire)
}

// RawGet retrieves a typed value from the cache using the specified key, without any additional logic.
// This method deserializes the values to defined type on retrieval.
//
// Warning:
// Use caution when using `RawGet`, as it does not respect any locks that may be in place on the cache entry.
// This could lead to reading stale or inconsistent data if another process is currently updating the value.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - key: The unique identifier for the cache entry.
//
// Returns:
//   - T: The typed value retrieved from the cache. If the key does not exist, a zero value for the type `T` is returned.
//   - error: If the cache operation or deserialization fails, an error is returned.
//
// Example:
// To directly retrieve a string value:
//
//	value, err := RawGet(ctx, "user_01J4YHWG45SC7VW684TZB2SZ7K")
//	if err != nil {
//	    log.Fatalf("Failed to get cache: %v", err)
//	}
//	log.Printf("Fetched value: %s", value)
func (tc *TypedCache[T]) RawGet(ctx context.Context, key string) (T, error) {
	var value T

	data, err := tc.cache.RawGet(ctx, key)
	if err != nil {
		return value, err
	}

	err = json.Unmarshal(data, &value)
	return value, err
}

// LockForUpdate locks a cache entry for update, ensuring that only one operation can modify the entry at a time.
// The lock is associated with a specific owner, which must be provided to unlock the entry later.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - key: The unique identifier for the cache entry to be locked.
//   - owner: The owner of the lock, used to identify the locking operation.
//
// Returns:
//   - error: If the cache operation fails, an error is returned.
//
// Example:
// To lock a cache entry for update:
//
//	err := LockForUpdate(ctx, "user_01J4YHWG45SC7VW684TZB2SZ7K", "service_01J4ZXVMKDFTSZGYKT5FXAZAB4")
//	if err != nil {
//	    log.Fatalf("Failed to lock cache key for update: %v", err)
//	}
func (tc *TypedCache[T]) LockForUpdate(ctx context.Context, key string, owner string) error {
	return tc.cache.LockForUpdate(ctx, key, owner)
}

// UnlockForUpdate unlocks a cache entry that was previously locked for update, allowing other operations to modify the entry.
// The owner of the lock must be provided to successfully unlock the entry.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - key: The unique identifier for the cache entry to be unlocked.
//   - owner: The owner of the lock, which was used to lock the entry.
//
// Returns:
//   - error: If the cache operation fails, an error is returned.
//
// Example:
// To unlock a cache entry after an update:
//
//	err := UnlockForUpdate(ctx, "user_01J4YHWG45SC7VW684TZB2SZ7K", "service_01J4ZXVMKDFTSZGYKT5FXAZAB4")
//	if err != nil {
//	    log.Fatalf("Failed to unlock cache key after update: %v", err)
//	}
func (tc *TypedCache[T]) UnlockForUpdate(ctx context.Context, key string, owner string) error {
	return tc.cache.UnlockForUpdate(ctx, key, owner)
}
