package zencache

import "github.com/redis/go-redis/v9"

// lock is a Redis Lua script used for acquiring a lock on a cache entry for a specific owner.
//
// The script performs the following operations:
//   - Checks the current lock status and lock owner.
//   - If no valid lock exists or the current owner matches the expected owner, it sets a new lock and ownership.
//   - Returns 'LOCKED' if the lock was successfully acquired or the current owner if the lock is held by someone else.
//
// Parameters used in the script:
//   - KEYS[1]: The key of the cache entry to be locked (Redis hash key).
//   - ARGV[1]: The owner of the lock (used to set lock ownership).
//   - ARGV[2]: The lock expiration time (used to set a new lock if needed).
//
// Example usage in Go:
//
//	status, err := runLua(ctx, redisClient, lock, []string{key}, []interface{}{owner, lockUntil})
//	if err != nil {
//	    log.Fatalf("Failed to acquire lock: %v", err)
//	}
//	if status == "LOCKED" {
//	    log.Println("Successfully acquired lock")
//	} else {
//	    log.Printf("Lock held by %s", status)
//	}
var lock = redis.NewScript(`
local lu = redis.call('HGET', KEYS[1], 'lockUntil')
local lo = redis.call('HGET', KEYS[1], 'lockOwner')
if lu == false or tonumber(lu) < tonumber(ARGV[2]) or lo == ARGV[1] then
	redis.call('HSET', KEYS[1], 'lockUntil', ARGV[2])
	redis.call('HSET', KEYS[1], 'lockOwner', ARGV[1])
	return 'LOCKED'
end
return lo`)

// unlock is a Redis Lua script used for releasing a lock on a cache entry if the caller is the current lock owner.
//
// The script performs the following operations:
//   - Checks if the current lock owner matches the expected owner.
//   - If the owner matches, clears the lock fields and sets an expiration time.
//
// Parameters used in the script:
//   - KEYS[1]: The key of the cache entry to be unlocked (Redis hash key).
//   - ARGV[1]: The owner of the lock (must match the current owner for the lock to be released).
//   - ARGV[2]: The expiration time in seconds to set for the cache entry.
//
// Example usage in Go:
//
//	err := runLua(ctx, redisClient, unlock, []string{key}, []interface{}{owner, int64(lockExpire / time.Second)})
//	if err != nil {
//	    log.Fatalf("Failed to release lock: %v", err)
//	}
var unlock = redis.NewScript(`
local lo = redis.call('HGET', KEYS[1], 'lockOwner')
if lo == ARGV[1] then
	redis.call('HSET', KEYS[1], 'lockUntil', 0)
	redis.call('HDEL', KEYS[1], 'lockOwner')
	redis.call('EXPIRE', KEYS[1], ARGV[2])
end`)

// setSingle is a Redis Lua script used for updating a single cache entry if the caller holds the lock on it.
//
// The script performs the following operations:
//   - Checks if the current lock owner matches the expected owner.
//   - If the lock owner matches, updates the value, clears the lock fields, and sets an expiration time.
//   - If the lock owner does not match, the script does nothing.
//
// Parameters used in the script:
//   - KEYS[1]: The key of the cache entry to be updated (Redis hash key).
//   - ARGV[1]: The new value to set.
//   - ARGV[2]: The expected owner of the lock (must match the current owner for the update to proceed).
//   - ARGV[3]: The expiration time in seconds to set for the cache entry.
//
// Example usage in Go:
//
//	err := runLua(ctx, redisClient, setSingle, []string{key}, []interface{}{newValue, owner, int64(expire / time.Second)})
//	if err != nil {
//	    log.Fatalf("Failed to set cache key: %v", err)
//	}
var setSingle = redis.NewScript(`
local o = redis.call('HGET', KEYS[1], 'lockOwner')
if o ~= ARGV[2] then
		return
end
redis.call('HSET', KEYS[1], 'value', ARGV[1])
redis.call('HDEL', KEYS[1], 'lockUntil')
redis.call('HDEL', KEYS[1], 'lockOwner')
redis.call('EXPIRE', KEYS[1], ARGV[3])`)

// getSingle is a Redis Lua script used for fetching a single cache entry while attempting to acquire a lock if necessary.
//
// The script performs the following operations:
//   - Retrieves the current value and lock information for the cache entry.
//   - If the lock is expired or not set, or if the value does not exist, it sets a new lock and returns the value with a 'LOCKED' status.
//   - If the lock is valid and the value exists, it returns the value and current lock information.
//
// Parameters used in the script:
//   - KEYS[1]: The key of the cache entry to be fetched (Redis hash key).
//   - ARGV[1]: The current time (used to check lock expiration).
//   - ARGV[2]: The lock expiration time (used to set a new lock if needed).
//   - ARGV[3]: The owner of the lock (used to set lock ownership).
//
// Example usage in Go:
//
//	value, status, err := runLua(ctx, redisClient, getSingle, []string{key}, []interface{}{currentTime, lockUntil, owner})
//	if err != nil {
//	    log.Fatalf("Failed to fetch cache key: %v", err)
//	}
//	log.Printf("Fetched value: %s, Status: %s", value, status)
var getSingle = redis.NewScript(`
local v = redis.call('HGET', KEYS[1], 'value')
local lu = redis.call('HGET', KEYS[1], 'lockUntil')
if lu ~= false and tonumber(lu) < tonumber(ARGV[1]) or lu == false and v == false then
	redis.call('HSET', KEYS[1], 'lockUntil', ARGV[2])
	redis.call('HSET', KEYS[1], 'lockOwner', ARGV[3])
	return { v, 'LOCKED' }
end
return {v, lu}`)

// deleteSingle is a Redis Lua script used for deleting a single cache entry by clearing its lock and setting an expiration time.
//
// The script performs the following operations:
//   - Clears the `lockUntil` field, effectively unlocking the entry.
//   - Removes the `lockOwner` field, releasing ownership of the lock.
//   - Sets an expiration time on the cache entry to ensure it is eventually deleted.
//
// Parameters used in the script:
//   - KEYS[1]: The key of the cache entry to be deleted (Redis hash key).
//   - ARGV[1]: The expiration time in seconds to set for the cache entry.
//
// Example usage in Go:
//
//	err := runLua(ctx, redisClient, deleteSingle, []string{key}, []interface{}{int64(expire / time.Second)})
//	if err != nil {
//	    log.Fatalf("Failed to delete cache key: %v", err)
//	}
var deleteSingle = redis.NewScript(`
redis.call('HSET', KEYS[1], 'lockUntil', 0)
redis.call('HDEL', KEYS[1], 'lockOwner')
redis.call('EXPIRE', KEYS[1], ARGV[1])`)

// setBatch is a Redis Lua script used for updating multiple cache entries if the caller holds the lock on each of them.
//
// The script performs the following operations for each key:
//   - Checks if the current lock owner matches the expected owner.
//   - If the lock owner matches, updates the value, clears the lock fields, and sets an expiration time.
//   - If the lock owner does not match, the script does nothing for that entry.
//
// Parameters used in the script:
//   - KEYS: A list of cache entry keys to be updated (Redis hash keys).
//   - ARGV[1]: The expected owner of the locks (must match the current owner for the updates to proceed).
//   - ARGV[2...]: The new values to set for the cache entries (corresponds to the keys in the same order).
//   - ARGV[n+2]: The expiration time in seconds to set for the cache entries (applies to all entries).
//
// Example usage in Go:
//
//	err := runLua(ctx, redisClient, setBatch, keys, []interface{}{owner, values, int64(expire / time.Second)})
//	if err != nil {
//	    log.Fatalf("Failed to set cache keys: %v", err)
//	}
var setBatch = redis.NewScript(`
local n = #KEYS
for i, key in ipairs(KEYS)
do
	local o = redis.call('HGET', key, 'lockOwner')
	if o ~= ARGV[1] then
			return
	end
	redis.call('HSET', key, 'value', ARGV[i+1])
	redis.call('HDEL', key, 'lockUntil')
	redis.call('HDEL', key, 'lockOwner')
	redis.call('EXPIRE', key, ARGV[i+1+n])
end`)

// getBatch is a Redis Lua script used for fetching multiple cache entries while attempting to acquire locks for them if necessary.
//
// The script performs the following operations for each key:
//   - Retrieves the current value and lock information.
//   - If the lock is expired or not set, or if the value does not exist, it sets a new lock and returns the value with a 'LOCKED' status.
//   - If the lock is valid and the value exists, it returns the value and current lock information.
//
// Parameters used in the script:
//   - KEYS: A list of cache entry keys to be fetched (Redis hash keys).
//   - ARGV[1]: The current time (used to check lock expiration).
//   - ARGV[2]: The lock expiration time (used to set a new lock if needed).
//   - ARGV[3]: The owner of the lock (used to set lock ownership).
//
// Example usage in Go:
//
//	results, err := runLua(ctx, redisClient, getBatch, keys, []interface{}{currentTime, lockUntil, owner})
//	if err != nil {
//	    log.Fatalf("Failed to fetch cache keys: %v", err)
//	}
//	for _, result := range results {
//	    value, status := result[0].(string), result[1].(string)
//	    log.Printf("Fetched value: %s, Status: %s", value, status)
//	}
var getBatch = redis.NewScript(`
local rets = {}
for i, key in ipairs(KEYS)
do
	local v = redis.call('HGET', key, 'value')
	local lu = redis.call('HGET', key, 'lockUntil')
	if lu ~= false and tonumber(lu) < tonumber(ARGV[1]) or lu == false and v == false then
		redis.call('HSET', key, 'lockUntil', ARGV[2])
		redis.call('HSET', key, 'lockOwner', ARGV[3])
		table.insert(rets, { v, 'LOCKED' })
	else
		table.insert(rets, {v, lu})
	end
end
return rets`)

// deleteBatch is a Redis Lua script used for deleting multiple cache entries by clearing their locks and setting expiration times.
//
// The script performs the following operations for each key:
//   - Clears the `lockUntil` field, effectively unlocking the entry.
//   - Removes the `lockOwner` field, releasing ownership of the lock.
//   - Sets an expiration time on each cache entry to ensure it is eventually deleted.
//
// Parameters used in the script:
//   - KEYS: A list of cache entry keys to be deleted (Redis hash keys).
//   - ARGV[1]: The expiration time in seconds to set for each cache entry.
//
// Example usage in Go:
//
//	err := runLua(ctx, redisClient, deleteBatch, keys, []interface{}{int64(expire / time.Second)})
//	if err != nil {
//	    log.Fatalf("Failed to delete cache keys: %v", err)
//	}
var deleteBatch = redis.NewScript(`
for i, key in ipairs(KEYS) do
	redis.call('HSET', key, 'lockUntil', 0)
	redis.call('HDEL', key, 'lockOwner')
	redis.call('EXPIRE', key, ARGV[1])
end`)
