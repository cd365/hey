// Querying data using cache.

package hey

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"strconv"
	"sync"
	"time"
)

// CacheQuery Cache queried data.
type CacheQuery interface {
	// Get Query cache data.
	Get(ctx context.Context, key string) (value interface{}, exists bool, err error)

	// Set Permanently cache data.
	Set(ctx context.Context, key string, value interface{}) error

	// SetTtl Set cache data, if the ttl(time ti live) is not a positive integer, the data should be stored for a long time or even permanently.
	SetTtl(ctx context.Context, key string, value interface{}, ttl time.Duration) error

	// Exist Cache data exist using key.
	Exist(ctx context.Context, key string) (exists bool, err error)

	// Remove Delete cache data using key.
	Remove(ctx context.Context, key string) error

	// Flush Empty the cache.
	Flush(ctx context.Context) error

	// Mutex Get a mutex lock based on the cache key.
	Mutex(key string) sync.Locker
}

// CacheValue Cache data with validity deadline millisecond timestamp.
type CacheValue struct {
	// UnixMilli Cache validity deadline millisecond timestamp.
	UnixMilli int64

	// Value Cache any data.
	Value interface{}
}

// MakeCacheQueryMutexIndex Calculate the mutex index for CacheQuery.
func MakeCacheQueryMutexIndex(key string, length int) int {
	if length <= 0 {
		panic("the length is at least 1")
	}
	sum := md5.Sum([]byte(key))
	tmp, err := strconv.ParseUint(hex.EncodeToString(sum[:])[:16], 16, 64)
	if err != nil {
		panic(err)
	}
	if index := int(tmp % uint64(length)); index >= 0 {
		return index
	}
	return 0
}
