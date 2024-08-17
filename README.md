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
go get github.com/driftdev/resilicache-go
```
