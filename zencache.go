package zencache

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

type Options struct {
	Delay                  time.Duration
	EmptyExpire            time.Duration
	LockExpire             time.Duration
	LockSleep              time.Duration
	WaitReplicas           int
	WaitReplicasTimeout    time.Duration
	RandomExpireAdjustment float64
	DisableCacheRead       bool
	DisableCacheDelete     bool
	StrongConsistency      bool
}

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

type Cache struct {
	client  redis.UniversalClient
	Options Options
	group   singleflight.Group
}

func NewCache(cache redis.UniversalClient, options Options) *Cache {
	if options.Delay == 0 || options.LockExpire == 0 {
		log.Fatal("cache options error: Delay and LockExpire should not be 0, you should call NewDefaultOptions() to get default options")
	}
	return &Cache{
		client:  cache,
		Options: options,
	}
}

func (c *Cache) Set(ctx context.Context, key string, value string, expire time.Duration) error {
	_, err := c.client.Set(ctx, key, value, expire).Result()
	if err != nil {
		return err
	}

	return nil
}

func (c *Cache) Get(ctx context.Context, key string) (string, error) {
	value, err := c.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	return value, nil
}

func (c *Cache) Delete(ctx context.Context, keys ...string) error {
	_, err := c.client.Del(ctx, keys...).Result()
	if err != nil {
		return err
	}

	return nil
}

func (c *Cache) FetchSingle(ctx context.Context, key string, expire time.Duration, fn func() (string, error)) (string, error) {
	ex := expire - c.Options.Delay - time.Duration(rand.Float64()*c.Options.RandomExpireAdjustment*float64(expire))
	v, err, _ := c.group.Do(key, func() (interface{}, error) {
		if c.Options.DisableCacheRead {
			return fn()
		} else if c.Options.StrongConsistency {
			return c.strongFetch(ctx, key, ex, fn)
		}
		return c.weakFetch(ctx, key, ex, fn)
	})

	return v.(string), err
}

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

func (c *Cache) FetchBatch(ctx context.Context, keys []string, expire time.Duration, fn func(indexes []int) (map[int]string, error)) (map[int]string, error) {
	if c.Options.DisableCacheRead {
		return fn(c.keysIndex(keys))
	} else if c.Options.StrongConsistency {
		return c.strongFetchBatch(ctx, keys, expire, fn)
	}
	return c.weakFetchBatch(ctx, keys, expire, fn)
}

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

func (c *Cache) RawGet(ctx context.Context, key string) (string, error) {
	return c.client.HGet(ctx, key, "value").Result()
}

func (c *Cache) RawSet(ctx context.Context, key string, value string, expire time.Duration) error {
	err := c.client.HSet(ctx, key, "value", value).Err()
	if err == nil {
		err = c.client.Expire(ctx, key, expire).Err()
	}

	return err
}

func (c *Cache) LockForUpdate(ctx context.Context, key string, owner string) error {
	lockUntil := math.Pow10(10)
	res, err := runLua(ctx, c.client, lock, []string{key}, []interface{}{owner, lockUntil})
	if err == nil && res != locked {
		return fmt.Errorf("%s has been locked by %s", key, res)
	}
	return err
}

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

func (c *Cache) luaSet(ctx context.Context, key string, value string, expire int, owner string) error {
	_, err := runLua(ctx, c.client, setSingle, []string{key}, []interface{}{value, owner, expire})
	return err
}

func (c *Cache) fetchNew(ctx context.Context, key string, expire time.Duration, owner string, fn func() (string, error)) (string, error) {
	result, err := fn()
	if err != nil {
		_ = c.UnlockForUpdate(ctx, key, owner)
		return "", err
	}
	if result == "" {
		if c.Options.EmptyExpire == 0 { // if empty expire is 0, then delete the key
			err = c.client.Del(ctx, key).Err()
			return "", err
		}
		expire = c.Options.EmptyExpire
	}
	err = c.luaSet(ctx, key, result, int(expire/time.Second), owner)
	return result, err
}

func (c *Cache) weakFetch(ctx context.Context, key string, expire time.Duration, fn func() (string, error)) (string, error) {
	owner := shortuuid.New()
	r, err := c.luaGet(ctx, key, owner)
	for err == nil && r[0] == nil && r[1].(string) != locked {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(c.Options.LockSleep):
		}
		r, err = c.luaGet(ctx, key, owner)
	}
	if err != nil {
		return "", err
	}
	if r[1] != locked {
		return r[0].(string), nil
	}
	if r[0] == nil {
		return c.fetchNew(ctx, key, expire, owner, fn)
	}
	go withRecover(func() {
		_, _ = c.fetchNew(ctx, key, expire, owner, fn)
	})
	return r[0].(string), nil
}

func (c *Cache) strongFetch(ctx context.Context, key string, expire time.Duration, fn func() (string, error)) (string, error) {
	owner := shortuuid.New()
	r, err := c.luaGet(ctx, key, owner)
	for err == nil && r[1] != nil && r[1] != locked { // locked by other
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(c.Options.LockSleep):
		}
		r, err = c.luaGet(ctx, key, owner)
	}
	if err != nil {
		return "", err
	}
	if r[1] != locked { // normal value
		return r[0].(string), nil
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

func (c *Cache) luaSetBatch(ctx context.Context, keys []string, values []string, expires []int, owner string) error {
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

func (c *Cache) fetchBatch(ctx context.Context, keys []string, indexes []int, expire time.Duration, owner string, fn func(indexes []int) (map[int]string, error)) (map[int]string, error) {
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
		data = make(map[int]string)
	}

	var batchKeys []string
	var batchValues []string
	var batchExpires []int

	for _, idx := range indexes {
		v := data[idx]
		ex := expire - c.Options.Delay - time.Duration(rand.Float64()*c.Options.RandomExpireAdjustment*float64(expire))
		if v == "" {
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
	data string
	err  error
}

func (c *Cache) weakFetchBatch(ctx context.Context, keys []string, expire time.Duration, fn func(indexes []int) (map[int]string, error)) (map[int]string, error) {
	var result = make(map[int]string)
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

		result[i] = r[0].(string)
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
					ch <- pair{idx: i, data: "", err: err}
					return
				}
				if r[1] != locked { // normal value
					ch <- pair{idx: i, data: r[0].(string), err: nil}
					return
				}
				if r[0] == nil {
					ch <- pair{idx: i, data: "", err: errNeedFetch}
					return
				}
				ch <- pair{idx: i, data: "", err: errNeedAsyncFetch}
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

func (c *Cache) strongFetchBatch(ctx context.Context, keys []string, expire time.Duration, fn func(indexes []int) (map[int]string, error)) (map[int]string, error) {
	var result = make(map[int]string)
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
			result[i] = r[0].(string)
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
					ch <- pair{idx: i, data: "", err: err}
					return
				}
				if r[1] != locked { // normal value
					ch <- pair{idx: i, data: r[0].(string), err: nil}
					return
				}
				// locked for update
				ch <- pair{idx: i, data: "", err: errNeedFetch}
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
