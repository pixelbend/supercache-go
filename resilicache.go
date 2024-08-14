package resilicache

import (
	"context"
	"errors"
	"fmt"
	"github.com/lithammer/shortuuid"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
	"log"
	"math"
	"math/rand"
	"runtime/debug"
	"sync"
	"time"
)

const locked = "LOCKED"

// Options holds the configuration settings for the Cache.
type Options struct {
	// Delay is the delay delete time for keys that are tag deleted. default is 10s
	Delay time.Duration
	// EmptyExpire is the expiry time for empty result. default is 60s
	EmptyExpire time.Duration
	// LockExpire is the expiry time for the lock which is allocated when updating cache. default is 3s
	// should be set to the max of the underling data calculating time.
	LockExpire time.Duration
	// LockSleep is the sleep interval time if try lock failed. default is 100ms
	LockSleep time.Duration
	// WaitReplicas is the number of replicas to wait for. default is 0
	// if WaitReplicas is > 0, it will use redis WAIT command to wait for TagAsDeleted synchronized.
	WaitReplicas int
	// WaitReplicasTimeout is the number of replicas to wait for. default is 3000ms
	// if WaitReplicas is > 0, WaitReplicasTimeout is the timeout for WAIT command.
	WaitReplicasTimeout time.Duration
	// RandomExpireAdjustment is the random adjustment for the expiry time. default 0.1
	// if the expiry time is set to 600s, and this value is set to 0.1, then the actual expire time will be 540s - 600s
	// solve the problem of cache avalanche.
	RandomExpireAdjustment float64
	// CacheReadDisabled is the flag to disable read cache. default is false
	// when redis is down, set this flat to downgrade.
	DisableCacheRead bool
	// CacheDeleteDisabled is the flag to disable delete cache. default is false
	// when redis is down, set this flat to downgrade.
	DisableCacheDelete bool
	// StrongConsistency is the flag to enable strong consistency. default is false
	// if enabled, the Fetch result will be consistent with the db result, but performance is bad.
	StrongConsistency bool
}

// NewDefaultOptions returns an Options struct initialized with default values for cache configuration settings.
// The default values are designed to provide a balanced configuration for typical caching scenarios.
// Each field in the Options struct is set to a default value, as documented below:
//
// Defaults:
//
//   - Delay: The time to delay the deletion of keys that are marked as deleted (tag deleted).
//     Default is 10 seconds.
//
//   - EmptyExpire: The expiration time for empty cache results (e.g., when a cache miss occurs and an empty result is cached).
//     Default is 60 seconds.
//
//   - LockExpire: The duration that a cache lock is held when updating the cache.
//     This should be set to the maximum time it takes to compute the underlying data.
//     Default is 3 seconds.
//
//   - LockSleep: The interval to sleep between retry attempts if acquiring a cache lock fails.
//     Default is 100 milliseconds.
//
//   - RandomExpireAdjustment: The random adjustment factor for cache expiry times.
//     For example, if the expiry time is set to 600 seconds and this value is set to 0.1,
//     the actual expiry time will be between 540 seconds and 600 seconds.
//     This helps mitigate cache avalanche issues.
//     Default is 0.1 (i.e., 10% adjustment).
//
//   - WaitReplicasTimeout: The maximum time to wait for replicas to synchronize when using the Redis WAIT command
//     if WaitReplicas is greater than 0.
//     Default is 3000 milliseconds (3 seconds).
func NewDefaultOptions() Options {
	return Options{
		Delay:                  10 * time.Second,
		EmptyExpire:            60 * time.Second,
		LockExpire:             3 * time.Second,
		LockSleep:              100 * time.Millisecond,
		RandomExpireAdjustment: 0.1,
		WaitReplicasTimeout:    3000 * time.Millisecond,
	}
}

// Cache is a struct that represents a caching system utilizing a Redis client.
// It encapsulates the redis.UniversalClient, cache configuration Options, and a singleflight.Group for managing
// duplicate requests.
type Cache struct {
	client  redis.UniversalClient
	Options Options
	group   singleflight.Group
}

// NewCache creates a new Cache instance with the provided Redis client and options.
// It initializes the Cache struct with the given Redis client and options, ensuring that critical fields
// such as Delay and LockExpire have valid (non-zero) values.
//
// Parameters:
//   - cache: A Redis client that implements the redis.UniversalClient interface, used to perform cache operations.
//   - options: An Options struct that holds various cache configuration settings.
//     If Delay or LockExpire is set to 0, the function will log a fatal error and terminate, advising to use NewDefaultOptions()
//     to get default values for these fields.
//
// Returns:
// - *Cache: A pointer to the newly created Cache instance.
//
// Usage:
// To create a new cache instance, first configure the Options using either NewDefaultOptions() or by specifying custom values.
// Then, pass the Redis client and the options to NewCache:
//
// Example:
//
//	opts := NewDefaultOptions()
//	redisClient := redis.NewUniversalClient(...)
//	cache := NewCache(redisClient, opts)
//
// If Delay or LockExpire is not properly set (i.e., is 0), the function will terminate with a fatal error,
// ensuring that these essential configuration values are provided.
func NewCache(cache redis.UniversalClient, options Options) *Cache {
	if options.Delay == 0 || options.LockExpire == 0 {
		log.Fatal("cache options error: Delay and LockExpire should not be 0, you should call NewDefaultOptions() to get default options")
	}
	return &Cache{
		client:  cache,
		Options: options,
	}
}

// Set stores a value in the cache under the specified key, with an associated expiration time.
// This method uses the Redis `SET` command to store the data, allowing it to expire after the
// given duration. If the operation fails, an error is returned.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - key: The unique identifier for the cache entry (e.g., cache key).
//   - value: The data to be stored in the cache, represented as a slice of bytes.
//   - expire: The duration after which the cache entry will expire and be automatically deleted.
//
// Returns:
//   - error: If the cache operation fails, an error is returned. Otherwise, it returns nil.
//
// Example:
//
//	err := Set(ctx, "user_01J4YHWG45SC7VW684TZB2SZ7K", []byte("example_value"), 10*time.Minute)
//	if err != nil {
//	    log.Fatalf("Failed to set cache: %v", err)
//	}
func (c *Cache) Set(ctx context.Context, key string, value []byte, expire time.Duration) error {
	_, err := c.client.Set(ctx, key, value, expire).Result()
	if err != nil {
		return err
	}

	return nil
}

// Get retrieves a value from the cache using the specified key.
// This method uses the Redis `GET` command to fetch the data stored under the given key.
// If the key exists, the stored value is returned as a slice of bytes.
// If the key does not exist (i.e., a cache miss), the method returns `nil` for both the value and the error.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - key: The unique identifier for the cache entry (e.g., cache key).
//
// Returns:
//   - []byte: The data retrieved from the cache, represented as a slice of bytes.
//     If the key does not exist, `nil` is returned.
//   - error: If the cache operation fails, an error is returned. Otherwise, it returns nil.
//
// Example:
//
//	dataBytes, err := Get(ctx, "user_01J4YHWG45SC7VW684TZB2SZ7K")
//	if err != nil {
//	    log.Fatalf("Failed to get cache: %v", err)
//	}
//	if data == nil {
//	    log.Println("Cache miss for key: user_01J4YHWG45SC7VW684TZB2SZ7K")
//	} else {
//	    log.Printf("Cache hit: %s", string(dataBytes))
//	}
func (c *Cache) Get(ctx context.Context, key string) ([]byte, error) {
	value, err := c.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return value, nil
}

// Delete removes one or more keys from the cache.
// This method uses the Redis `DEL` command to delete the specified keys from the cache.
// If the operation fails, an error is returned.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - keys: A variadic parameter that allows passing one or more cache keys to delete.
//
// Returns:
//   - error: If the cache operation fails, an error is returned. Otherwise, it returns nil.
//
// Example:
//
//	err := Delete(ctx, "user_01J4YHWG45SC7VW684TZB2SZ7K", "user_01J4YJESQBVN1CPV9EPWJJFJ7V")
//	if err != nil {
//	    log.Fatalf("Failed to delete cache keys: %v", err)
//	} else {
//	    log.Println("Cache keys deleted successfully")
//	}
func (c *Cache) Delete(ctx context.Context, keys ...string) error {
	_, err := c.client.Del(ctx, keys...).Result()
	if err != nil {
		return err
	}

	return nil
}

// FetchSingle retrieves a value from the cache, or if not present, retrieves the value using the provided function `fn`
// and stores it in the cache. This method ensures that only one request for the given key is processed at a time
// (using the singleflight pattern) to avoid duplicate work.
//
// The method supports both strong and weak consistency modes, and the choice between them is controlled by the `Options.StrongConsistency`
// configuration. Additionally, cache reads can be disabled globally through the `Options.DisableCacheRead`.
//
// The expiration time for the cache entry is adjusted using the configured `Options.Delay` and `Options.RandomExpireAdjustment`
// options to help mitigate issues like cache avalanches.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - key: The unique identifier for the cache entry (e.g., cache key).
//   - expire: The duration after which the cache entry will expire and be automatically deleted.
//   - fn: A function that computes the value to be cached if it is not already present.
//     This function is called only if the cache miss occurs or if the cache read is disabled.
//
// Returns:
//   - []byte: The data retrieved from the cache or computed by the function `fn`, represented as a slice of bytes.
//   - error: If the cache operation or the function `fn` fails, an error is returned.
//
// Example:
//
//	data, err := FetchSingle(ctx, "user_01J4YHWG45SC7VW684TZB2SZ7K", 10*time.Minute, func() ([]byte, error) {
//	    // Simulate data retrieval, e.g., from a database
//	    return fetchDataFromDatabase("user_01J4YHWG45SC7VW684TZB2SZ7K")
//	})
//	if err != nil {
//	    log.Fatalf("Failed to fetch data: %v", err)
//	}
//
//	log.Printf("Fetched data: %s", string(data))
func (c *Cache) FetchSingle(ctx context.Context, key string, expire time.Duration, fn func() ([]byte, error)) ([]byte, error) {
	ex := expire - c.Options.Delay - time.Duration(rand.Float64()*c.Options.RandomExpireAdjustment*float64(expire))
	v, err, _ := c.group.Do(key, func() (interface{}, error) {
		if c.Options.DisableCacheRead {
			return fn()
		} else if c.Options.StrongConsistency {
			return c.strongFetch(ctx, key, ex, fn)
		}
		return c.weakFetch(ctx, key, ex, fn)
	})

	return v.([]byte), err
}

// TagAsDeletedSingle marks a cache entry as deleted by setting a delayed deletion in Redis.
// The actual deletion is performed using a Lua script that delays the removal by the duration specified
// in the `Options.Delay` field. This is useful for scenarios where you want to invalidate a cache entry
// without immediately removing it, allowing other systems to recognize the deletion and take appropriate action.
//
// If cache deletion is disabled using `Options.DisableCacheDelete`, the method returns immediately without
// performing any operation.
//
// The method also supports waiting for a specified number of replicas to acknowledge the operation
// using the Redis `WAIT` command, if `Options.WaitReplicas` is greater than 0. This ensures stronger consistency
// across distributed Redis instances.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - key: The unique identifier for the cache entry to be marked as deleted.
//
// Returns:
//   - error: If an error occurs during the execution of the Lua script, processing the `WAIT` command,
//     or if the number of replicas acknowledging the operation is less than `Options.WaitReplicas`, an error is returned.
//     If cache deletion is disabled, it returns nil.
//
// Example:
//
//	err := TagAsDeletedSingle(ctx, "user_01J4YHWG45SC7VW684TZB2SZ7K")
//	if err != nil {
//	    log.Fatalf("Failed to tag cache key as deleted: %v", err)
//	} else {
//	    log.Println("Cache key tagged for delayed deletion successfully")
//	}
func (c *Cache) TagAsDeletedSingle(ctx context.Context, key string) error {
	if c.Options.DisableCacheDelete {
		return nil
	}

	luaFn := func(con redis.Scripter) error {
		_, err := runLua(ctx, con, deleteSingle, []string{key}, []interface{}{int64(c.Options.Delay / time.Second)})
		return err
	}
	if c.Options.WaitReplicas > 0 {
		err := luaFn(c.client)
		cmd := redis.NewCmd(ctx, "WAIT", c.Options.WaitReplicas, c.Options.WaitReplicasTimeout)
		if err == nil {
			err = c.client.Process(ctx, cmd)
		}
		var replicas int
		if err == nil {
			replicas, err = cmd.Int()
		}
		if err == nil && replicas < c.Options.WaitReplicas {
			err = fmt.Errorf("wait replicas %d failed. result replicas: %d", c.Options.WaitReplicas, replicas)
		}
		return err
	}

	return luaFn(c.client)
}

// FetchBatch retrieves multiple values from the cache, or if not present, computes them using the provided function `fn`
// and stores them in the cache. This method handles batch operations, allowing you to fetch or compute multiple values
// associated with the provided keys at once.
//
// The method supports both strong and weak consistency modes, and the choice between them is controlled by the `Options.StrongConsistency`
// configuration. Additionally, cache reads can be disabled globally through the `Options.DisableCacheRead`.
//
// The expiration time for the cache entry is adjusted using the configured `Options.Delay` and `Options.RandomExpireAdjustment`
// options to help mitigate issues like cache avalanches.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - keys: A slice of strings representing the unique identifiers for the cache entries (e.g., cache keys).
//   - expire: The duration after which the cache entries will expire and be automatically deleted.
//   - fn: A function that computes the values to be cached for the keys that are not already present in the cache.
//     The function receives a slice of indexes corresponding to the keys that need to be computed
//     and returns a map where the keys are the indexes from the input slice and the values are the
//     computed byte slices.
//
// Returns:
//   - map[int][]byte: A map where the keys are the indexes of the input keys slice, and the values are the
//     corresponding data retrieved from the cache or computed by the function `fn`.
//   - error: If the cache operation or the function `fn` fails, an error is returned.
//
// Example:
//
//	// Define a function to compute the data for missing keys
//	fn := func(indexes []int) (map[int][]byte, error) {
//	    result := make(map[int][]byte)
//	    for _, idx := range indexes {
//	        data, err := fetchDataFromDatabase(keys[idx])
//	        if err != nil {
//	            return nil, err
//	        }
//	        result[idx] = data
//	    }
//	    return result, nil
//	}
//
//	// Fetch a batch of Data
//	batchData, err := FetchBatch(ctx, []string{"user_01J4YHWG45SC7VW684TZB2SZ7K", "user_01J4YJESQBVN1CPV9EPWJJFJ7V"}, 10*time.Minute, fn)
//	if err != nil {
//	    log.Fatalf("Failed to fetch batch data: %v", err)
//	}
//
//	for idx, val := range data {
//	    log.Printf("Fetched data for key[%d]: %s", idx, string(val))
//	}
func (c *Cache) FetchBatch(ctx context.Context, keys []string, expire time.Duration, fn func(indexes []int) (map[int][]byte, error)) (map[int][]byte, error) {
	if c.Options.DisableCacheRead {
		return fn(c.keysIndex(keys))
	} else if c.Options.StrongConsistency {
		return c.strongFetchBatch(ctx, keys, expire, fn)
	}
	return c.weakFetchBatch(ctx, keys, expire, fn)
}

// TagAsDeletedBatch marks multiple cache entries as deleted by setting a delayed deletion in Redis.
// The actual deletion is performed using a Lua script that delays the removal by the duration specified
// in the `Options.Delay` field. This is useful for scenarios where you want to invalidate multiple cache entries
// without immediately removing them, allowing other systems to recognize the deletion and take appropriate action.
//
// If cache deletion is disabled using `Options.DisableCacheDelete`, the method returns immediately without
// performing any operation.
//
// The method also supports waiting for a specified number of replicas to acknowledge the operation
// using the Redis `WAIT` command, if `Options.WaitReplicas` is greater than 0. This ensures stronger consistency
// across distributed Redis instances.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - keys: A slice of strings representing the unique identifiers for the cache entries to be marked as deleted.
//
// Returns:
//   - error: If an error occurs during the execution of the Lua script, processing the `WAIT` command,
//     or if the number of replicas acknowledging the operation is less than `WaitReplicas`, an error is returned.
//     If cache deletion is disabled, it returns nil.
//
// Example:
//
//	err := cache.TagAsDeletedBatch(ctx, []string{"user_01J4YHWG45SC7VW684TZB2SZ7K", "user_01J4YJESQBVN1CPV9EPWJJFJ7V"})
//	if err != nil {
//	    log.Fatalf("Failed to tag cache keys as deleted: %v", err)
//	} else {
//	    log.Println("Cache keys tagged for delayed deletion successfully")
//	}
func (c *Cache) TagAsDeletedBatch(ctx context.Context, keys []string) error {
	if c.Options.DisableCacheDelete {
		return nil
	}
	luaFn := func(con redis.Scripter) error {
		_, err := runLua(ctx, con, deleteBatch, keys, []interface{}{int64(c.Options.Delay / time.Second)})
		return err
	}
	if c.Options.WaitReplicas > 0 {
		err := luaFn(c.client)
		cmd := redis.NewCmd(ctx, "WAIT", c.Options.WaitReplicas, c.Options.WaitReplicasTimeout)
		if err == nil {
			err = c.client.Process(ctx, cmd)
		}
		var replicas int
		if err == nil {
			replicas, err = cmd.Int()
		}
		if err == nil && replicas < c.Options.WaitReplicas {
			err = fmt.Errorf("wait replicas %d failed. result replicas: %d", c.Options.WaitReplicas, replicas)
		}
		return err
	}
	return luaFn(c.client)
}

// RawGet directly retrieves the value of a specific cache entry from a Redis hash by its key.
// This method bypasses any locking mechanisms, meaning it can read data that might currently be locked for updates.
//
// Warning:
// Use caution when using `RawGet`, as it does not respect any locks that may be in place on the cache entry.
// This could lead to reading stale or inconsistent data if another process is currently updating the value.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - key: The unique identifier for the cache entry (e.g., the Redis hash key).
//
// Returns:
//   - string: The value associated with the given key in the Redis hash.
//   - error: If an error occurs during the retrieval process, it is returned.
//
// Example:
//
//	value, err := RawGet(ctx, "user_01J4YHWG45SC7VW684TZB2SZ7K")
//	if err != nil {
//	    log.Fatalf("Failed to retrieve cache key: %v", err)
//	}
//	log.Printf("Retrieved value: %s", value)
func (c *Cache) RawGet(ctx context.Context, key string) ([]byte, error) {
	return c.client.HGet(ctx, key, "value").Bytes()
}

// RawSet directly stores a value in a specific cache entry in a Redis hash and sets an expiration time for the entry.
// This method bypasses any locking mechanisms, meaning it can overwrite data that might currently be locked for updates.
//
// Warning:
// Use caution when using `RawSet`, as it does not respect any locks that may be in place on the cache entry.
// This could lead to race conditions or overwriting data that is currently being updated by another process.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - key: The unique identifier for the cache entry (e.g., the Redis hash key).
//   - value: The value to be stored in the "value" field of the Redis hash.
//   - expire: The duration after which the cache entry will expire and be automatically deleted.
//
// Returns:
//   - error: If an error occurs during the storage or expiration process, it is returned.
//
// Example:
//
//	err := RawSet(ctx, "user_01J4YHWG45SC7VW684TZB2SZ7K", []byte("example_value"), 10*time.Minute)
//	if err != nil {
//	    log.Fatalf("Failed to set cache key: %v", err)
//	}
//	log.Println("Cache key set successfully")
func (c *Cache) RawSet(ctx context.Context, key string, value []byte, expire time.Duration) error {
	err := c.client.HSet(ctx, key, "value", value).Err()
	if err == nil {
		err = c.client.Expire(ctx, key, expire).Err()
	}

	return err
}

// LockForUpdate attempts to acquire a lock on a cache entry for a specific owner, allowing
// the owner to perform updates without interference. The lock is implemented using a Lua script
// that sets a lock with a very high expiration time (`math.Pow10(10)`).
//
// If the lock is already held by another owner, the method returns an error indicating
// the key is locked by someone else.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - key: The unique identifier for the cache entry (e.g., the Redis key to be locked).
//   - owner: A string representing the entity (e.g., a unique identifier) that requests the lock.
//
// Returns:
//   - error: If an error occurs during the locking process or if the lock is already held by another owner, it is returned.
//
// Example:
//
//	err := LockForUpdate(ctx, "user_01J4YHWG45SC7VW684TZB2SZ7K", "service_01J4ZXVMKDFTSZGYKT5FXAZAB4")
//	if err != nil {
//	    log.Fatalf("Failed to lock cache key: %v", err)
//	}
//	log.Println("Cache key locked for update")
func (c *Cache) LockForUpdate(ctx context.Context, key string, owner string) error {
	lockUntil := math.Pow10(10)
	res, err := runLua(ctx, c.client, lock, []string{key}, []interface{}{owner, lockUntil})
	if err == nil && res != locked {
		return fmt.Errorf("%s has been locked by %s", key, res)
	}
	return err
}

// UnlockForUpdate releases a lock on a cache entry for a specific owner, allowing other entities
// to acquire the lock. The lock is removed using a Lua script that checks the ownership
// and then releases the lock if the owner matches.
//
// Parameters:
//   - ctx: The context to control cancellation and timeouts.
//   - key: The unique identifier for the cache entry (e.g., the Redis key to be unlocked).
//   - owner: A string representing the entity (e.g., a unique identifier) that currently holds the lock.
//
// Returns:
//   - error: If an error occurs during the unlocking process, it is returned.
//
// Example:
//
//	err := UnlockForUpdate(ctx, "user_01J4YHWG45SC7VW684TZB2SZ7K", "service_01J4ZXVMKDFTSZGYKT5FXAZAB4")
//	if err != nil {
//	    log.Fatalf("Failed to unlock cache key: %v", err)
//	}
//	log.Println("Cache key unlocked for update")
func (c *Cache) UnlockForUpdate(ctx context.Context, key string, owner string) error {
	_, err := runLua(ctx, c.client, unlock, []string{key}, []interface{}{owner, c.Options.LockExpire / time.Second})
	return err
}

func (c *Cache) luaGet(ctx context.Context, key string, owner string) ([]interface{}, error) {
	res, err := runLua(ctx, c.client, getSingle, []string{key}, []interface{}{now(), now() + int64(c.Options.LockExpire/time.Second), owner})
	if err != nil {
		return nil, err
	}
	return res.([]interface{}), nil
}

func (c *Cache) luaSet(ctx context.Context, key string, value []byte, expire int, owner string) error {
	_, err := runLua(ctx, c.client, setSingle, []string{key}, []interface{}{value, owner, expire})
	return err
}

func (c *Cache) fetchNew(ctx context.Context, key string, expire time.Duration, owner string, fn func() ([]byte, error)) ([]byte, error) {
	result, err := fn()
	if err != nil {
		_ = c.UnlockForUpdate(ctx, key, owner)
		return nil, err
	}
	if result == nil {
		if c.Options.EmptyExpire == 0 { // if empty expire is 0, then delete the key
			err = c.client.Del(ctx, key).Err()
			return nil, err
		}
		expire = c.Options.EmptyExpire
	}
	err = c.luaSet(ctx, key, result, int(expire/time.Second), owner)
	return result, err
}

func (c *Cache) weakFetch(ctx context.Context, key string, expire time.Duration, fn func() ([]byte, error)) ([]byte, error) {
	owner := shortuuid.New()
	r, err := c.luaGet(ctx, key, owner)
	for err == nil && r[0] == nil && r[1].(string) != locked {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(c.Options.LockSleep):
		}
		r, err = c.luaGet(ctx, key, owner)
	}
	if err != nil {
		return nil, err
	}
	if r[1] != locked {
		return r[0].([]byte), nil
	}
	if r[0] == nil {
		return c.fetchNew(ctx, key, expire, owner, fn)
	}
	go withRecover(func() {
		_, _ = c.fetchNew(ctx, key, expire, owner, fn)
	})
	return r[0].([]byte), nil
}

func (c *Cache) strongFetch(ctx context.Context, key string, expire time.Duration, fn func() ([]byte, error)) ([]byte, error) {
	owner := shortuuid.New()
	r, err := c.luaGet(ctx, key, owner)
	for err == nil && r[1] != nil && r[1] != locked { // locked by other
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(c.Options.LockSleep):
		}
		r, err = c.luaGet(ctx, key, owner)
	}
	if err != nil {
		return nil, err
	}
	if r[1] != locked { // normal value
		return r[0].([]byte), nil
	}
	return c.fetchNew(ctx, key, expire, owner, fn)
}

var (
	errNeedFetch      = errors.New("need fetch")
	errNeedAsyncFetch = errors.New("need async fetch")
)

func (c *Cache) luaGetBatch(ctx context.Context, keys []string, owner string) ([]interface{}, error) {
	res, err := runLua(ctx, c.client, getBatch, keys, []interface{}{now(), now() + int64(c.Options.LockExpire/time.Second), owner})
	if err != nil {
		return nil, err
	}
	return res.([]interface{}), nil
}

func (c *Cache) luaSetBatch(ctx context.Context, keys []string, values [][]byte, expires []int, owner string) error {
	var vals = make([]interface{}, 0, 2+len(values))
	vals = append(vals, owner)
	for _, v := range values {
		vals = append(vals, v)
	}
	for _, ex := range expires {
		vals = append(vals, ex)
	}
	_, err := runLua(ctx, c.client, setBatch, keys, vals)
	return err
}

func (c *Cache) fetchBatch(ctx context.Context, keys []string, indexes []int, expire time.Duration, owner string, fn func(indexes []int) (map[int][]byte, error)) (map[int][]byte, error) {
	defer func() {
		if r := recover(); r != nil {
			debug.PrintStack()
		}
	}()
	data, err := fn(indexes)
	if err != nil {
		for _, idx := range indexes {
			_ = c.UnlockForUpdate(ctx, keys[idx], owner)
		}
		return nil, err
	}

	if data == nil {
		data = make(map[int][]byte)
	}

	var batchKeys []string
	var batchValues [][]byte
	var batchExpires []int

	for _, idx := range indexes {
		v := data[idx]
		ex := expire - c.Options.Delay - time.Duration(rand.Float64()*c.Options.RandomExpireAdjustment*float64(expire))
		if v == nil {
			if c.Options.EmptyExpire == 0 { // if empty expire is 0, then delete the key
				_ = c.client.Del(ctx, keys[idx]).Err()
				continue
			}
			ex = c.Options.EmptyExpire

			data[idx] = v // in case idx not in data
		}
		batchKeys = append(batchKeys, keys[idx])
		batchValues = append(batchValues, v)
		batchExpires = append(batchExpires, int(ex/time.Second))
	}

	err = c.luaSetBatch(ctx, batchKeys, batchValues, batchExpires, owner)

	return data, nil
}

func (c *Cache) keysIndex(keys []string) (indexes []int) {
	for i := range keys {
		indexes = append(indexes, i)
	}
	return indexes
}

type pair struct {
	idx  int
	data []byte
	err  error
}

func (c *Cache) weakFetchBatch(ctx context.Context, keys []string, expire time.Duration, fn func(indexes []int) (map[int][]byte, error)) (map[int][]byte, error) {
	var result = make(map[int][]byte)
	owner := shortuuid.New()
	var toGet, toFetch, toFetchAsync []int

	// read from redis without sleep
	rs, err := c.luaGetBatch(ctx, keys, owner)
	if err != nil {
		return nil, err
	}
	for i, v := range rs {
		r := v.([]interface{})

		if r[0] == nil {
			if r[1] == locked {
				toFetch = append(toFetch, i)
			} else {
				toGet = append(toGet, i)
			}
			continue
		}

		if r[1] == locked {
			toFetchAsync = append(toFetchAsync, i)
			// fallthrough with old data
		} // else new data

		result[i] = r[0].([]byte)
	}

	if len(toFetchAsync) > 0 {
		go func(indexes []int) {
			_, _ = c.fetchBatch(ctx, keys, indexes, expire, owner, fn)
		}(toFetchAsync)
		toFetchAsync = toFetchAsync[:0] // reset toFetch
	}

	if len(toFetch) > 0 {
		// batch fetch
		fetched, err := c.fetchBatch(ctx, keys, toFetch, expire, owner, fn)
		if err != nil {
			return nil, err
		}
		for _, k := range toFetch {
			result[k] = fetched[k]
		}
		toFetch = toFetch[:0] // reset toFetch
	}

	if len(toGet) > 0 {
		// read from redis and sleep to wait
		var wg sync.WaitGroup

		var ch = make(chan pair, len(toGet))
		for _, idx := range toGet {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				r, err := c.luaGet(ctx, keys[i], owner)
				for err == nil && r[0] == nil && r[1].(string) != locked {
					select {
					case <-ctx.Done():
						ch <- pair{idx: i, err: ctx.Err()}
						return
					case <-time.After(c.Options.LockSleep):
						// equal to time.Sleep(c.Options.LockSleep) but can be canceled
					}
					r, err = c.luaGet(ctx, keys[i], owner)
				}
				if err != nil {
					ch <- pair{idx: i, data: nil, err: err}
					return
				}
				if r[1] != locked { // normal value
					ch <- pair{idx: i, data: r[0].([]byte), err: nil}
					return
				}
				if r[0] == nil {
					ch <- pair{idx: i, data: nil, err: errNeedFetch}
					return
				}
				ch <- pair{idx: i, data: nil, err: errNeedAsyncFetch}
			}(idx)
		}
		wg.Wait()
		close(ch)

		for p := range ch {
			if p.err != nil {
				switch {
				case errors.Is(p.err, errNeedFetch):
					toFetch = append(toFetch, p.idx)
					continue
				case errors.Is(p.err, errNeedAsyncFetch):
					toFetchAsync = append(toFetchAsync, p.idx)
					continue
				default:
				}
				return nil, p.err
			}
			result[p.idx] = p.data
		}
	}

	if len(toFetchAsync) > 0 {
		go func(indexes []int) {
			_, _ = c.fetchBatch(ctx, keys, indexes, expire, owner, fn)
		}(toFetchAsync)
	}

	if len(toFetch) > 0 {
		// batch fetch
		fetched, err := c.fetchBatch(ctx, keys, toFetch, expire, owner, fn)
		if err != nil {
			return nil, err
		}
		for _, k := range toFetch {
			result[k] = fetched[k]
		}
	}

	return result, nil
}

func (c *Cache) strongFetchBatch(ctx context.Context, keys []string, expire time.Duration, fn func(indexes []int) (map[int][]byte, error)) (map[int][]byte, error) {
	var result = make(map[int][]byte)
	owner := shortuuid.New()
	var toGet, toFetch []int

	// read from redis without sleep
	rs, err := c.luaGetBatch(ctx, keys, owner)
	if err != nil {
		return nil, err
	}
	for i, v := range rs {
		r := v.([]interface{})
		if r[1] == nil { // normal value
			result[i] = r[0].([]byte)
			continue
		}

		if r[1] != locked { // locked by other
			toGet = append(toGet, i)
			continue
		}

		// locked for fetch
		toFetch = append(toFetch, i)
	}

	if len(toFetch) > 0 {
		// batch fetch
		fetched, err := c.fetchBatch(ctx, keys, toFetch, expire, owner, fn)
		if err != nil {
			return nil, err
		}
		for _, k := range toFetch {
			result[k] = fetched[k]
		}
		toFetch = toFetch[:0] // reset toFetch
	}

	if len(toGet) > 0 {
		// read from redis and sleep to wait
		var wg sync.WaitGroup
		var ch = make(chan pair, len(toGet))
		for _, idx := range toGet {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				r, err := c.luaGet(ctx, keys[i], owner)
				for err == nil && r[1] != nil && r[1] != locked { // locked by other
					select {
					case <-ctx.Done():
						ch <- pair{idx: i, err: ctx.Err()}
						return
					case <-time.After(c.Options.LockSleep):
						// equal to time.Sleep(c.Options.LockSleep) but can be canceled
					}
					r, err = c.luaGet(ctx, keys[i], owner)
				}
				if err != nil {
					ch <- pair{idx: i, data: nil, err: err}
					return
				}
				if r[1] != locked { // normal value
					ch <- pair{idx: i, data: r[0].([]byte), err: nil}
					return
				}
				// locked for update
				ch <- pair{idx: i, data: nil, err: errNeedFetch}
			}(idx)
		}
		wg.Wait()
		close(ch)
		for p := range ch {
			if p.err != nil {
				if errors.Is(p.err, errNeedFetch) {
					toFetch = append(toFetch, p.idx)
					continue
				}
				return nil, p.err
			}
			result[p.idx] = p.data
		}
	}

	if len(toFetch) > 0 {
		// batch fetch
		fetched, err := c.fetchBatch(ctx, keys, toFetch, expire, owner, fn)
		if err != nil {
			return nil, err
		}
		for _, k := range toFetch {
			result[k] = fetched[k]
		}
	}

	return result, nil
}
