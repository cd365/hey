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

// Cache Querying data using cache.
type Cache interface {
	// Locker Get a mutex lock based on the cache key
	Locker(key string) sync.Locker

	// DelCtx Delete cache data
	DelCtx(ctx context.Context, key string) error

	// GetCtx Query cache data
	GetCtx(ctx context.Context, key string) (value interface{}, exists bool, err error)

	// SetCtx Set cache data, if the duration is not a positive integer, the data should be stored for a long time or even permanently.
	SetCtx(ctx context.Context, key string, value interface{}, duration ...time.Duration) error
}

type cache struct {
	locker    map[int]sync.Locker
	lockerCap int
	del       func(ctx context.Context, key string) error
	get       func(ctx context.Context, key string) (interface{}, bool, error)
	set       func(ctx context.Context, key string, value interface{}, duration ...time.Duration) error
}

func (s *cache) index(key string) int {
	sum := md5.Sum([]byte(key))
	tmp, _ := strconv.ParseInt(hex.EncodeToString(sum[:])[:8], 16, 64)
	if index := int(tmp % int64(s.lockerCap)); index >= 0 {
		return index
	}
	return 0
}

func (s *cache) Locker(key string) sync.Locker {
	return s.locker[s.index(key)]
}

func (s *cache) DelCtx(ctx context.Context, key string) error {
	return s.del(ctx, key)
}

func (s *cache) GetCtx(ctx context.Context, key string) (interface{}, bool, error) {
	return s.get(ctx, key)
}

func (s *cache) SetCtx(ctx context.Context, key string, value interface{}, duration ...time.Duration) error {
	return s.set(ctx, key, value, duration...)
}

func NewCache(
	lockers int, // number of cache key hash locker
	del func(ctx context.Context, key string) error,
	get func(ctx context.Context, key string) (interface{}, bool, error),
	set func(ctx context.Context, key string, value interface{}, duration ...time.Duration) error,
) Cache {
	if lockers <= 0 {
		lockers = 1024
	}
	tmp := &cache{
		lockerCap: lockers,
		locker:    make(map[int]sync.Locker, lockers),
		del:       del,
		get:       get,
		set:       set,
	}
	for i := 0; i < lockers; i++ {
		tmp.locker[i] = &sync.Mutex{}
	}
	return tmp
}
