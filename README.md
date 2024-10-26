# ResiliCache Go

ResiliCache Go is a robust and versatile caching package for Go, compatible with various caching systems, including Redis, RedisCluster, ValKey, KeyDB, DragonflyDB, and Kvrocks. 
It provides a comprehensive set of features to enhance the reliability and performance of your caching strategy.

## Features

- **Eventual Consistency**: Guarantees eventual consistency of cache data even under extreme conditions, ensuring that your application eventually reflects the most up-to-date information.

- **Strong Consistency**: Offers strong consistency guarantees for cache access, making sure that your application always interacts with the most recent and reliable data.

- **Anti-Breakdown**: Implements strategies to prevent cache breakdown, minimizing the risk of performance degradation during high traffic or system failures.

- **Anti-Penetration**: Provides mechanisms to protect your cache from excessive load caused by cache penetration attacks, where requests bypass the cache and hit the database directly.

- **Anti-Avalanche**: Uses techniques to prevent cache avalanches, which occur when many cache entries expire simultaneously, potentially overwhelming your backend systems.

- **Batch Query**: Supports batch querying to efficiently handle multiple cache requests in a single operation, reducing the overhead and latency of individual cache accesses.

## Installation

To install ResiliCache Go, use the following command:

```bash
go get github.com/teapartydev/resilicache-go
```

## Usage

Here's a basic examples of how to use ResiliCache Go.
 
```go
package main

import (
	"context"
	"fmt"
	"github.com/teapartydev/resilicache-go"
	"github.com/redis/go-redis/v9"
	"log"
	"time"
)

func main() {
	// Initialize Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	// Flush the Redis database to start with a clean state
	if err := rdb.FlushDB(context.Background()).Err(); err != nil {
		log.Fatalf("Error flushing DB: %v", err)
	}

	// Initialize ResiliCache with Redis client and default options
	cache := resilicache.NewCache(rdb, resilicache.NewDefaultOptions())

	// Set a key-value pair in the cache with a TTL of 10 seconds
	err := cache.Set(context.Background(), "user:01J61BPPHMFH9VSF2T1R2ZXDA2", []byte("tester"), time.Second*10)
	if err != nil {
		log.Fatalf("Error setting key: %v", err)
	}

	// Retrieve the value for the key from the cache
	value, err := cache.Get(context.Background(), "user:01J61BPPHMFH9VSF2T1R2ZXDA2")
	if err != nil {
		log.Fatalf("Error getting key: %v", err)
	}
	if value == nil {
		log.Fatalf("Error: value not found")
	}
	fmt.Println("value: ", string(value))

	// Delete the key from the cache
	err = cache.Delete(context.Background(), "user:01J61BPPHMFH9VSF2T1R2ZXDA2")
	if err != nil {
		log.Fatalf("Error deleting key: %v", err)
	}

	// Fetch data for the key, caching it if it's not already present
	// If the key is not found in the cache, the provided function is called to get the data
	value, err = cache.FetchSingle(context.Background(), "user:01J61BPPHMFH9VSF2T1R2ZXDA2", time.Second*10, func() ([]byte, error) {
		// Fetch data from an external source (e.g., database)
		return []byte("tester"), nil
	})
	if err != nil {
		log.Fatalf("Error fetching key: %v", err)
	}
	fmt.Println("value: ", string(value))

	// Mark the key as deleted, indicating it should not be retrieved from the cache.
	// After being marked, the key is permanently deleted with a delay.
	err = cache.TagAsDeletedSingle(context.Background(), "user:01J61BPPHMFH9VSF2T1R2ZXDA2")
	if err != nil {
		log.Fatalf("Error tagging key as deleted: %v", err)
	}

	// Fetch data for multiple keys, caching them if they are not already present
	// If any keys are not found in the cache, the provided function is called to get the data
	values, err := cache.FetchBatch(context.Background(),
		[]string{
			"user:01J61C9MEMNXYKWJXNQ3ERQA1F",
			"user:01J61CH3XEVGKHGK3GKA7XJGF9",
			"user:01J61CHNPXZ65T9EZ3RSHJJDKV",
		},
		time.Second*10,
		func(idxs []int) (map[int][]byte, error) {
			// Fetch data from an external source for each key (e.g., database)
			values := make(map[int][]byte)
			for _, i := range idxs {
				values[i] = []byte(fmt.Sprintf("tester_%d", i))
			}
			return values, nil
		},
	)
	if err != nil {
		log.Fatalf("Error fetching keys: %v", err)
	}
	for _, v := range values {
		fmt.Println("value: ", string(v))
	}

	// Mark multiple keys as deleted, indicating they should not be retrieved from the cache.
	// After being marked, these keys are permanently deleted with a delay.
	err = cache.TagAsDeletedBatch(context.Background(),
		[]string{
			"user:01J61C9MEMNXYKWJXNQ3ERQA1F",
			"user:01J61CH3XEVGKHGK3GKA7XJGF9",
			"user:01J61CHNPXZ65T9EZ3RSHJJDKV",
		},
	)
	if err != nil {
		log.Fatalf("Error tagging keys as deleted: %v", err)
	}
}
```

Here's a basic examples of how to use ResiliCache TypedCache.
This leverages Go generics to enforce type safety, ensuring that only 
specified types are stored and retried.

```go
package main

import (
	"context"
	"fmt"
	"github.com/driftdev/resilicache-go"
	"github.com/redis/go-redis/v9"
	"log"
	"time"
)

func main() {
	// Initialize Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	// Flush the Redis database to start with a clean state
	if err := rdb.FlushDB(context.Background()).Err(); err != nil {
		log.Fatalf("Error flushing DB: %v", err)
	}

	// Initialize ResiliCache with Redis client and default options
	cache := resilicache.NewCache(rdb, resilicache.NewDefaultOptions())

	// Initialize a TypedCache instance with string as the value type
	// This enforces type safety, ensuring that only strings are stored and retrieved from the cache
	typedCache := resilicache.NewTypedCache[string](cache)

	// Set a key-value pair in the cache with a TTL of 10 seconds
	err := typedCache.Set(context.Background(), "user:01J61BPPHMFH9VSF2T1R2ZXDA2", "tester", time.Second*10)
	if err != nil {
		log.Fatalf("Error setting key: %v", err)
	}

	// Retrieve the value for the key from the cache
	// The retrieved value is already typed as a string
	value, err := typedCache.Get(context.Background(), "user:01J61BPPHMFH9VSF2T1R2ZXDA2")
	if err != nil {
		log.Fatalf("Error getting key: %v", err)
	}
	if value == "" {
		log.Fatalf("Error: value not found")
	}
	fmt.Println("value: ", value)

	// Delete the key from the cache
	err = typedCache.Delete(context.Background(), "user:01J61BPPHMFH9VSF2T1R2ZXDA2")
	if err != nil {
		log.Fatalf("Error deleting key: %v", err)
	}

	// Fetch data for the key, caching it if it's not already present
	// If the key is not found in the cache, the provided function is called to generate the data
	value, err = typedCache.FetchSingle(context.Background(), "user:01J61BPPHMFH9VSF2T1R2ZXDA2", time.Second*10, func() (string, error) {
		// Fetch or generate data (e.g., from a database)
		return "tester", nil
	})
	if err != nil {
		log.Fatalf("Error fetching key: %v", err)
	}
	fmt.Println("value: ", value)

	// Mark the key as deleted, so it should not be retrieved from the cache
	// The key is permanently deleted after a delay
	err = typedCache.TagAsDeletedSingle(context.Background(), "user:01J61BPPHMFH9VSF2T1R2ZXDA2")
	if err != nil {
		log.Fatalf("Error tagging key as deleted: %v", err)
	}

	// Fetch data for multiple keys, caching them if not already present
	// If any keys are missing from the cache, the provided function is called to generate the data
	values, err := typedCache.FetchBatch(context.Background(),
		[]string{
			"user:01J61C9MEMNXYKWJXNQ3ERQA1F",
			"user:01J61CH3XEVGKHGK3GKA7XJGF9",
			"user:01J61CHNPXZ65T9EZ3RSHJJDKV",
		},
		time.Second*10,
		func(idxs []int) (map[int]string, error) {
			// Fetch or generate data for each key (e.g., from a database)
			values := make(map[int]string)
			for _, i := range idxs {
				values[i] = fmt.Sprintf("tester_%d", i)
			}
			return values, nil
		},
	)
	if err != nil {
		log.Fatalf("Error fetching keys: %v", err)
	}
	for _, v := range values {
		fmt.Println("value: ", v)
	}

	// Mark multiple keys as deleted, so they should not be retrieved from the cache
	// The keys are permanently deleted after a delay
	err = typedCache.TagAsDeletedBatch(context.Background(),
		[]string{
			"user:01J61C9MEMNXYKWJXNQ3ERQA1F",
			"user:01J61CH3XEVGKHGK3GKA7XJGF9",
			"user:01J61CHNPXZ65T9EZ3RSHJJDKV",
		},
	)
	if err != nil {
		log.Fatalf("Error tagging keys as deleted: %v", err)
	}
}
```