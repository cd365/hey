// Cache query data to reduce database pressure; Used in combination with query functions to reduce coupling.
// Supports fast extraction of specific types of data from the cache, including bool, int, float, string ...

package hey

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/cd365/hey/v6/cst"
)

const (
	// ErrNoDataInCache No data in cache.
	ErrNoDataInCache = Err("hey: no data in cache")
)

// Cacher Cache interface.
type Cacher interface {
	// Key Customize cache key processing before reading and writing cache.
	Key(key string) string

	// Get Reading data from the cache.
	Get(ctx context.Context, key string) ([]byte, error)

	// Set Writing data to the cache.
	Set(ctx context.Context, key string, value []byte, duration ...time.Duration) error

	// Del Deleting data from the cache.
	Del(ctx context.Context, key string) error

	// Marshal Serialize cache data.
	Marshal(v any) ([]byte, error)

	// Unmarshal Deserialize cache data.
	Unmarshal(data []byte, v any) error
}

// Cache Read and write data in cache.
type Cache struct {
	cacher Cacher
}

// NewCache Create a new *Cache object.
func NewCache(cacher Cacher) *Cache {
	if cacher == nil {
		panic("hey: cacher is nil")
	}
	return &Cache{
		cacher: cacher,
	}
}

// GetCacher Read Cacher.
func (s *Cache) GetCacher() Cacher {
	return s.cacher
}

// SetCacher Write Cacher.
func (s *Cache) SetCacher(cacher Cacher) *Cache {
	if cacher != nil {
		s.cacher = cacher
	}
	return s
}

// Get Read cache data from cache.
func (s *Cache) Get(ctx context.Context, key string) ([]byte, error) {
	key = s.cacher.Key(key)
	return s.cacher.Get(ctx, key)
}

// Set Write cache data to cache.
func (s *Cache) Set(ctx context.Context, key string, value []byte, duration ...time.Duration) error {
	key = s.cacher.Key(key)
	return s.cacher.Set(ctx, key, value, duration...)
}

// Del Deleting data from the cache.
func (s *Cache) Del(ctx context.Context, key string) error {
	key = s.cacher.Key(key)
	return s.cacher.Del(ctx, key)
}

// GetUnmarshal Read cached data from the cache and deserialize cached data.
func (s *Cache) GetUnmarshal(ctx context.Context, key string, value any) error {
	tmp, err := s.Get(ctx, key)
	if err != nil {
		return err
	}
	return s.cacher.Unmarshal(tmp, value)
}

// MarshalSet Serialize cache data and write the serialized data to the cache.
func (s *Cache) MarshalSet(ctx context.Context, key string, value any, duration ...time.Duration) error {
	tmp, err := s.cacher.Marshal(value)
	if err != nil {
		return err
	}
	return s.Set(ctx, key, tmp, duration...)
}

// GetString Read string cache data from cache.
func (s *Cache) GetString(ctx context.Context, key string) (string, error) {
	result, err := s.Get(ctx, key)
	if err != nil {
		return cst.Empty, err
	}
	return string(result), nil
}

// SetString Write string cache data to cache.
func (s *Cache) SetString(ctx context.Context, key string, value string, duration ...time.Duration) error {
	return s.Set(ctx, key, []byte(value), duration...)
}

// GetFloat Read float64 cache data from cache.
func (s *Cache) GetFloat(ctx context.Context, key string) (float64, error) {
	result, err := s.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	f64, err := strconv.ParseFloat(string(result), 64)
	if err != nil {
		return 0, err
	}
	return f64, nil
}

// SetFloat Write float64 cache data to cache.
func (s *Cache) SetFloat(ctx context.Context, key string, value float64, duration ...time.Duration) error {
	return s.Set(ctx, key, []byte(strconv.FormatFloat(value, 'f', -1, 64)), duration...)
}

// GetInt Read int64 cache data from cache.
func (s *Cache) GetInt(ctx context.Context, key string) (int64, error) {
	result, err := s.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	i64, err := strconv.ParseInt(string(result), 10, 64)
	if err != nil {
		return 0, err
	}
	return i64, nil
}

// SetInt Write int64 cache data to cache.
func (s *Cache) SetInt(ctx context.Context, key string, value int64, duration ...time.Duration) error {
	return s.Set(ctx, key, []byte(strconv.FormatInt(value, 10)), duration...)
}

// GetBool Read bool cache data from cache.
func (s *Cache) GetBool(ctx context.Context, key string) (bool, error) {
	result, err := s.Get(ctx, key)
	if err != nil {
		return false, err
	}
	boolean, err := strconv.ParseBool(string(result))
	if err != nil {
		return false, err
	}
	return boolean, nil
}

// SetBool Write bool cache data to cache.
func (s *Cache) SetBool(ctx context.Context, key string, value bool, duration ...time.Duration) error {
	return s.Set(ctx, key, fmt.Appendf(nil, "%t", value), duration...)
}

// DurationRange Get a random Duration between minValue*duration and maxValue*duration.
func (s *Cache) DurationRange(duration time.Duration, minValue int, maxValue int) time.Duration {
	return time.Duration(minValue+rand.IntN(maxValue-minValue+1)) * duration
}

// CacheMaker Cache SQL statement related data, including but not limited to cache query data.
type CacheMaker interface {
	// GetCacheKey Use prepare and args to calculate the hash value as the cache key.
	GetCacheKey() (string, error)

	// UseCacheKey Custom build cache key.
	UseCacheKey(cacheKey func(maker Maker) (string, error)) CacheMaker

	// Reset For reset maker and it's related property values.
	Reset(maker ...Maker) CacheMaker

	// Get For get value from cache.
	Get(ctx context.Context) ([]byte, error)

	// Set For set value to cache.
	Set(ctx context.Context, value []byte, duration ...time.Duration) error

	// Del Delete data in the cache based on cache key.
	Del(ctx context.Context) error

	// GetUnmarshal Query data and unmarshal data.
	GetUnmarshal(ctx context.Context, value any) error

	// MarshalSet Marshal data and set data.
	MarshalSet(ctx context.Context, value any, duration ...time.Duration) error

	// GetString Get string type value.
	GetString(ctx context.Context) (string, error)

	// SetString Set string type value.
	SetString(ctx context.Context, value string, duration ...time.Duration) error

	// GetFloat Get float64 type value.
	GetFloat(ctx context.Context) (float64, error)

	// SetFloat Set float64 type value.
	SetFloat(ctx context.Context, value float64, duration ...time.Duration) error

	// GetInt Get int64 type value.
	GetInt(ctx context.Context) (int64, error)

	// SetInt Set int64 type value.
	SetInt(ctx context.Context, value int64, duration ...time.Duration) error

	// GetBool Get boolean type value.
	GetBool(ctx context.Context) (bool, error)

	// SetBool Set boolean type value.
	SetBool(ctx context.Context, value bool, duration ...time.Duration) error
}

// cacheMaker Implementing the CacheMaker interface.
type cacheMaker struct {
	// cache Instance of Cache.
	cache *Cache

	// maker Cache Maker Object.
	maker Maker

	// cacheKey Allows custom unique cache keys to be constructed based on query objects.
	cacheKey func() (string, error)

	// key Cache key.
	key string
}

// NewCacheMaker Create a new CacheMaker object.
func NewCacheMaker(cache *Cache, maker Maker) CacheMaker {
	if cache == nil || maker == nil {
		return nil
	}
	return &cacheMaker{
		cache: cache,
		maker: maker,
	}
}

// getCacheKey Default method for building cache key.
func (s *cacheMaker) getCacheKey() (string, error) {
	if s.key != cst.Empty {
		return s.key, nil
	}

	script := s.maker.ToSQL()
	if script.IsEmpty() {
		return cst.Empty, ErrEmptyScript
	}

	for index, value := range script.Args {
		if tmp, ok := value.([]byte); ok && tmp != nil {
			script.Args[index] = hex.EncodeToString(tmp)
		}
	}
	args, err := s.cache.GetCacher().Marshal(script.Args)
	if err != nil {
		return cst.Empty, err
	}

	b := poolGetStringBuilder()
	defer poolPutStringBuilder(b)

	b.WriteString(script.Prepare)
	b.WriteString(";")
	b.Write(args)

	hash := sha256.New()
	if _, err = hash.Write([]byte(b.String())); err != nil {
		return cst.Empty, err
	}
	s.key = hex.EncodeToString(hash.Sum(nil))
	return s.key, nil
}

// GetCacheKey Build cache key, custom method is used first.
func (s *cacheMaker) GetCacheKey() (string, error) {
	cacheKey := s.cacheKey
	if cacheKey == nil {
		return s.getCacheKey()
	}
	if s.key != cst.Empty {
		return s.key, nil
	}
	key, err := cacheKey()
	if err != nil {
		return cst.Empty, err
	}
	s.key = key
	return s.key, nil
}

// UseCacheKey Using custom method to build cache key.
func (s *cacheMaker) UseCacheKey(cacheKey func(maker Maker) (string, error)) CacheMaker {
	if cacheKey != nil {
		s.cacheKey = func() (string, error) { return cacheKey(s.maker) }
	}
	return s
}

// Reset Resetting cache related properties.
func (s *cacheMaker) Reset(maker ...Maker) CacheMaker {
	s.key = cst.Empty
	for _, tmp := range maker {
		if tmp != nil {
			s.maker = tmp
			break
		}
	}
	return s
}

// Get Read data from cache.
func (s *cacheMaker) Get(ctx context.Context) ([]byte, error) {
	if _, err := s.GetCacheKey(); err != nil {
		return nil, err
	}
	return s.cache.Get(ctx, s.key)
}

// Set Write data to cache.
func (s *cacheMaker) Set(ctx context.Context, value []byte, duration ...time.Duration) error {
	if _, err := s.GetCacheKey(); err != nil {
		return err
	}
	return s.cache.Set(ctx, s.key, value, duration...)
}

// Del Delete cache value.
func (s *cacheMaker) Del(ctx context.Context) error {
	if _, err := s.GetCacheKey(); err != nil {
		return err
	}
	return s.cache.Del(ctx, s.key)
}

// GetUnmarshal Get cached value and deserialize.
func (s *cacheMaker) GetUnmarshal(ctx context.Context, value any) error {
	if _, err := s.GetCacheKey(); err != nil {
		return err
	}
	return s.cache.GetUnmarshal(ctx, s.key, value)
}

// MarshalSet Serialize cache data and set serialized data to the cache.
func (s *cacheMaker) MarshalSet(ctx context.Context, value any, duration ...time.Duration) error {
	if _, err := s.GetCacheKey(); err != nil {
		return err
	}
	return s.cache.MarshalSet(ctx, s.key, value, duration...)
}

// GetString Get string value.
func (s *cacheMaker) GetString(ctx context.Context) (string, error) {
	if _, err := s.GetCacheKey(); err != nil {
		return cst.Empty, err
	}
	return s.cache.GetString(ctx, s.key)
}

// SetString Set string value.
func (s *cacheMaker) SetString(ctx context.Context, value string, duration ...time.Duration) error {
	if _, err := s.GetCacheKey(); err != nil {
		return err
	}
	return s.cache.SetString(ctx, s.key, value, duration...)
}

// GetFloat Get float64 value.
func (s *cacheMaker) GetFloat(ctx context.Context) (float64, error) {
	if _, err := s.GetCacheKey(); err != nil {
		return 0, err
	}
	return s.cache.GetFloat(ctx, s.key)
}

// SetFloat Set float64 value.
func (s *cacheMaker) SetFloat(ctx context.Context, value float64, duration ...time.Duration) error {
	if _, err := s.GetCacheKey(); err != nil {
		return err
	}
	return s.cache.SetFloat(ctx, s.key, value, duration...)
}

// GetInt Get int64 value.
func (s *cacheMaker) GetInt(ctx context.Context) (int64, error) {
	if _, err := s.GetCacheKey(); err != nil {
		return 0, err
	}
	return s.cache.GetInt(ctx, s.key)
}

// SetInt Set int64 value.
func (s *cacheMaker) SetInt(ctx context.Context, value int64, duration ...time.Duration) error {
	if _, err := s.GetCacheKey(); err != nil {
		return err
	}
	return s.cache.SetInt(ctx, s.key, value, duration...)
}

// GetBool Get bool value.
func (s *cacheMaker) GetBool(ctx context.Context) (bool, error) {
	if _, err := s.GetCacheKey(); err != nil {
		return false, err
	}
	return s.cache.GetBool(ctx, s.key)
}

// SetBool Set bool value.
func (s *cacheMaker) SetBool(ctx context.Context, value bool, duration ...time.Duration) error {
	if _, err := s.GetCacheKey(); err != nil {
		return err
	}
	return s.cache.SetBool(ctx, s.key, value, duration...)
}

// StringMutex maps string keys to a fixed set of sync.Mutex locks using hashing.
type StringMutex struct {
	// mutexes Slice of mutexes, fixed after initialization.
	mutexes []*sync.Mutex

	// length Number of mutexes, fixed after initialization.
	length int
}

// NewStringMutex creates a new StringMutex with the specified number of mutexes.
// If length is invalid (< 1 or > math.MaxUint16), it defaults to 256.
func NewStringMutex(length int) *StringMutex {
	if length < 1 || length > math.MaxUint16 {
		length = 256
	}
	result := &StringMutex{
		length:  length,
		mutexes: make([]*sync.Mutex, length),
	}
	for i := range result.mutexes {
		result.mutexes[i] = &sync.Mutex{}
	}
	return result
}

// Get returns the sync.Mutex corresponding to the given key.
func (s *StringMutex) Get(key string) *sync.Mutex {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	value := h.Sum64()
	index := value % uint64(s.length)
	return s.mutexes[index]
}

// Len returns the number of mutexes.
func (s *StringMutex) Len() int {
	return s.length
}

type MinMaxDuration struct {
	minValue int // Range minimum value.

	maxValue int // Range maximum value.

	duration time.Duration // Base duration value.
}

// NewMinMaxDuration The minimum value of all values should be granter than 0, unless you want to cache permanently.
func NewMinMaxDuration(duration time.Duration, minValue int, maxValue int) *MinMaxDuration {
	return (&MinMaxDuration{}).init(duration, minValue, maxValue)
}

func (s *MinMaxDuration) init(duration time.Duration, minValue int, maxValue int) *MinMaxDuration {
	if maxValue < minValue {
		minValue, maxValue = maxValue, minValue
	}
	s.duration, s.minValue, s.maxValue = duration, minValue, maxValue
	return s
}

func (s *MinMaxDuration) Get() time.Duration {
	if s.duration <= 0 || s.minValue <= 0 || s.maxValue <= 0 {
		return time.Duration(0)
	}
	return s.duration * time.Duration(s.minValue+rand.IntN(s.maxValue-s.minValue+1))
}
