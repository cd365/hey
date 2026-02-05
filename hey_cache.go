// Cache query data to reduce database pressure; Used in combination with query functions to reduce coupling.
// Supports fast extraction of specific types of data from the cache, including bool, int, float, string ...

package hey

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cd365/hey/v7/cst"
)

const (
	// ErrNoDataInCache No data in cache.
	ErrNoDataInCache = Err("hey: no data in cache")

	// ErrEmptyCacheKey Empty cache key.
	ErrEmptyCacheKey = Err("hey: empty cache key")
)

// Cacher The caching interface needs to be implemented to facilitate quick use of the cache.
type Cacher interface {
	// Key Customize cache key processing before reading and writing cache.
	Key(key string) string

	// Get Reading data from the cache, if the data does not exist, an ErrNoDataInCache error should be returned.
	Get(ctx context.Context, key string) ([]byte, error)

	// Set Writing data to the cache, a cache duration of 0 or a negative number indicates a permanent cache.
	Set(ctx context.Context, key string, value []byte, duration time.Duration) error

	// Del Deleting data from the cache.
	Del(ctx context.Context, key string) error

	// Has Does the report cache contain data corresponding to a certain key?
	Has(ctx context.Context, key string) (bool, error)

	// Marshal Serialize cache data.
	Marshal(v any) ([]byte, error)

	// Unmarshal Deserialize cache data.
	Unmarshal(data []byte, v any) error
}

// Cache Read and write data in cache.
type Cache struct {
	cacher Cacher
}

// NewCache Create a new *Cache object, to facilitate quick operation of cached data.
func NewCache(cacher Cacher) *Cache {
	if cacher == nil {
		panic(errors.New("hey: cacher is nil"))
	}
	return &Cache{
		cacher: cacher,
	}
}

// GetCacher Read Cacher.
func (s *Cache) GetCacher() Cacher {
	return s.cacher
}

// SetCacher Write Cacher, the current method is typically called during the initialization phase.
func (s *Cache) SetCacher(cacher Cacher) *Cache {
	if cacher != nil {
		s.cacher = cacher
	}
	return s
}

// Get Read cache data from cache.
func (s *Cache) Get(ctx context.Context, key string) ([]byte, error) {
	cacheKey := s.cacher.Key(key)
	return s.cacher.Get(ctx, cacheKey)
}

// Set Write cache data to cache.
func (s *Cache) Set(ctx context.Context, key string, value []byte, duration time.Duration) error {
	cacheKey := s.cacher.Key(key)
	return s.cacher.Set(ctx, cacheKey, value, duration)
}

// Del Deleting data from the cache.
func (s *Cache) Del(ctx context.Context, key string) error {
	cacheKey := s.cacher.Key(key)
	return s.cacher.Del(ctx, cacheKey)
}

// Has Does the report cache contain data corresponding to a certain key?
func (s *Cache) Has(ctx context.Context, key string) (bool, error) {
	cacheKey := s.cacher.Key(key)
	return s.cacher.Has(ctx, cacheKey)
}

// GetUnmarshal Read cached data from the cache and deserialize cached data.
func (s *Cache) GetUnmarshal(ctx context.Context, key string, value any) error {
	data, err := s.Get(ctx, key)
	if err != nil {
		return err
	}
	return s.cacher.Unmarshal(data, value)
}

// MarshalSet Serialize cache data and write the serialized data to the cache.
func (s *Cache) MarshalSet(ctx context.Context, key string, value any, duration time.Duration) error {
	data, err := s.cacher.Marshal(value)
	if err != nil {
		return err
	}
	return s.Set(ctx, key, data, duration)
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
func (s *Cache) SetString(ctx context.Context, key string, value string, duration time.Duration) error {
	return s.Set(ctx, key, []byte(value), duration)
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
func (s *Cache) SetFloat(ctx context.Context, key string, value float64, duration time.Duration) error {
	return s.Set(ctx, key, []byte(strconv.FormatFloat(value, 'f', -1, 64)), duration)
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
func (s *Cache) SetInt(ctx context.Context, key string, value int64, duration time.Duration) error {
	return s.Set(ctx, key, []byte(strconv.FormatInt(value, 10)), duration)
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
func (s *Cache) SetBool(ctx context.Context, key string, value bool, duration time.Duration) error {
	return s.Set(ctx, key, fmt.Appendf(nil, "%t", value), duration)
}

// RangeRandomDuration Get a random Duration between minValue*baseDuration and maxValue*baseDuration.
func (s *Cache) RangeRandomDuration(baseDuration time.Duration, minValue int, maxValue int) time.Duration {
	return time.Duration(minValue+rand.IntN(maxValue-minValue+1)) * baseDuration
}

// Maker New CacheMaker.
func (s *Cache) Maker(maker Maker) CacheMaker {
	return NewCacheMaker(s, maker)
}

// CacheMaker Cache SQL statement related data, including but not limited to cache query data.
type CacheMaker interface {
	// UseCacheKey Custom build cache key, the current method is typically called during the initialization phase.
	UseCacheKey(cacheKey func(maker Maker) (string, error)) CacheMaker

	// GetCacheKey Use prepare and args to calculate the hash value as the cache key.
	GetCacheKey() (string, error)

	// Get For get value from cache.
	Get(ctx context.Context) ([]byte, error)

	// Set For set value to cache.
	Set(ctx context.Context, value []byte, duration time.Duration) error

	// Del Delete data in the cache based on cache key.
	Del(ctx context.Context) error

	// Has Does the report cache contain data corresponding to a certain key?
	Has(ctx context.Context) (bool, error)

	// GetUnmarshal Get data and unmarshal data.
	GetUnmarshal(ctx context.Context, value any) error

	// MarshalSet Marshal data and set data.
	MarshalSet(ctx context.Context, value any, duration time.Duration) error

	// GetString Get string type value.
	GetString(ctx context.Context) (string, error)

	// SetString Set string type value.
	SetString(ctx context.Context, value string, duration time.Duration) error

	// GetFloat Get float64 type value.
	GetFloat(ctx context.Context) (float64, error)

	// SetFloat Set float64 type value.
	SetFloat(ctx context.Context, value float64, duration time.Duration) error

	// GetInt Get int64 type value.
	GetInt(ctx context.Context) (int64, error)

	// SetInt Set int64 type value.
	SetInt(ctx context.Context, value int64, duration time.Duration) error

	// GetBool Get boolean type value.
	GetBool(ctx context.Context) (bool, error)

	// SetBool Set boolean type value.
	SetBool(ctx context.Context, value bool, duration time.Duration) error
}

// cacheMaker Implementing the CacheMaker interface.
type cacheMaker struct {
	// cache Instance of *Cache.
	cache *Cache

	// maker Cache Maker object.
	maker Maker

	// cacheKey Allows custom unique cache keys to be constructed based on query objects.
	cacheKey func(maker Maker) (string, error)

	// key Cache key.
	key string
}

// NewCacheMaker Create a new CacheMaker object.
func NewCacheMaker(cache *Cache, maker Maker) CacheMaker {
	if cache == nil {
		panic(errors.New("hey: cache is nil"))
	}
	result := &cacheMaker{
		cache: cache,
		maker: maker,
	}
	result.cacheKey = result.getCacheKey
	return result
}

// getCacheKey Default method for building cache key.
func (s *cacheMaker) getCacheKey(maker Maker) (string, error) {
	if maker == nil {
		return cst.Empty, ErrInvalidMaker
	}
	script := maker.ToSQL()
	if script.IsEmpty() {
		return cst.Empty, ErrEmptySqlStatement
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
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// UseCacheKey Using custom method to build cache key.
func (s *cacheMaker) UseCacheKey(cacheKey func(maker Maker) (string, error)) CacheMaker {
	if cacheKey != nil {
		s.cacheKey = cacheKey
	}
	return s
}

// GetCacheKey Build cache key, custom method is used first.
func (s *cacheMaker) GetCacheKey() (string, error) {
	if s.key != cst.Empty {
		return s.key, nil
	}
	key, err := s.cacheKey(s.maker)
	if err != nil {
		return cst.Empty, err
	}
	key = strings.TrimSpace(key)
	if key == cst.Empty {
		return cst.Empty, ErrEmptyCacheKey
	}
	s.key = key
	return key, nil
}

// Get Read data from cache.
func (s *cacheMaker) Get(ctx context.Context) ([]byte, error) {
	key, err := s.GetCacheKey()
	if err != nil {
		return nil, err
	}
	return s.cache.Get(ctx, key)
}

// Set Write data to cache.
func (s *cacheMaker) Set(ctx context.Context, value []byte, duration time.Duration) error {
	key, err := s.GetCacheKey()
	if err != nil {
		return err
	}
	return s.cache.Set(ctx, key, value, duration)
}

// Del Delete cache value.
func (s *cacheMaker) Del(ctx context.Context) error {
	key, err := s.GetCacheKey()
	if err != nil {
		return err
	}
	return s.cache.Del(ctx, key)
}

// Has Does the report cache contain data corresponding to a certain key?
func (s *cacheMaker) Has(ctx context.Context) (bool, error) {
	key, err := s.GetCacheKey()
	if err != nil {
		return false, err
	}
	return s.cache.Has(ctx, key)
}

// GetUnmarshal Get cached value and deserialize.
func (s *cacheMaker) GetUnmarshal(ctx context.Context, value any) error {
	key, err := s.GetCacheKey()
	if err != nil {
		return err
	}
	return s.cache.GetUnmarshal(ctx, key, value)
}

// MarshalSet Serialize cache data and set serialized data to the cache.
func (s *cacheMaker) MarshalSet(ctx context.Context, value any, duration time.Duration) error {
	key, err := s.GetCacheKey()
	if err != nil {
		return err
	}
	return s.cache.MarshalSet(ctx, key, value, duration)
}

// GetString Get string value.
func (s *cacheMaker) GetString(ctx context.Context) (string, error) {
	key, err := s.GetCacheKey()
	if err != nil {
		return cst.Empty, err
	}
	return s.cache.GetString(ctx, key)
}

// SetString Set string value.
func (s *cacheMaker) SetString(ctx context.Context, value string, duration time.Duration) error {
	key, err := s.GetCacheKey()
	if err != nil {
		return err
	}
	return s.cache.SetString(ctx, key, value, duration)
}

// GetFloat Get float64 value.
func (s *cacheMaker) GetFloat(ctx context.Context) (float64, error) {
	key, err := s.GetCacheKey()
	if err != nil {
		return 0, err
	}
	return s.cache.GetFloat(ctx, key)
}

// SetFloat Set float64 value.
func (s *cacheMaker) SetFloat(ctx context.Context, value float64, duration time.Duration) error {
	key, err := s.GetCacheKey()
	if err != nil {
		return err
	}
	return s.cache.SetFloat(ctx, key, value, duration)
}

// GetInt Get int64 value.
func (s *cacheMaker) GetInt(ctx context.Context) (int64, error) {
	key, err := s.GetCacheKey()
	if err != nil {
		return 0, err
	}
	return s.cache.GetInt(ctx, key)
}

// SetInt Set int64 value.
func (s *cacheMaker) SetInt(ctx context.Context, value int64, duration time.Duration) error {
	key, err := s.GetCacheKey()
	if err != nil {
		return err
	}
	return s.cache.SetInt(ctx, key, value, duration)
}

// GetBool Get bool value.
func (s *cacheMaker) GetBool(ctx context.Context) (bool, error) {
	key, err := s.GetCacheKey()
	if err != nil {
		return false, err
	}
	return s.cache.GetBool(ctx, key)
}

// SetBool Set bool value.
func (s *cacheMaker) SetBool(ctx context.Context, value bool, duration time.Duration) error {
	key, err := s.GetCacheKey()
	if err != nil {
		return err
	}
	return s.cache.SetBool(ctx, key, value, duration)
}

// MultiMutex Maps string keys to a fixed set of *sync.Mutex locks using hashing.
type MultiMutex interface {
	// Get returns the sync.Mutex corresponding to the given key.
	Get(key string) *sync.Mutex

	// Len returns the number of mutexes.
	Len() int
}

type multiMutex struct {
	// mutexes Slice of mutexes, fixed after initialization.
	mutexes []*sync.Mutex

	// length Number of mutexes, fixed after initialization.
	length int
}

// NewMultiMutex creates a new MultiMutex with the specified number of mutexes.
// If length is invalid (< 1 or > math.MaxUint16), it defaults to 256.
func NewMultiMutex(length int) MultiMutex {
	if length < 1 || length > math.MaxUint16 {
		length = 256
	}
	result := &multiMutex{
		length:  length,
		mutexes: make([]*sync.Mutex, length),
	}
	for i := range result.mutexes {
		result.mutexes[i] = &sync.Mutex{}
	}
	return result
}

func (s *multiMutex) Get(key string) *sync.Mutex {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	value := h.Sum64()
	index := value % uint64(s.length)
	return s.mutexes[index]
}

func (s *multiMutex) Len() int {
	return s.length
}

// RangeRandomDuration Get a random duration within a range.
type RangeRandomDuration interface {
	// Get a random duration.
	Get() time.Duration

	// GetBaseDuration Get base duration.
	GetBaseDuration() time.Duration

	// SetBaseDuration Set base duration.
	SetBaseDuration(baseDuration time.Duration) RangeRandomDuration

	// GetRange Get duration range.
	GetRange() (int, int)

	// SetRange Set duration range.
	SetRange(minValue int, maxValue int) RangeRandomDuration
}

type rangeRandomDuration struct {
	minValue int // Range minimum value.

	maxValue int // Range maximum value.

	baseDuration time.Duration // Base duration value.
}

// NewRangeRandomDuration The minimum value of all values should be granter than 0, unless you want to cache permanently.
func NewRangeRandomDuration(baseDuration time.Duration, minValue int, maxValue int) RangeRandomDuration {
	return (&rangeRandomDuration{}).init(baseDuration, minValue, maxValue)
}

func (s *rangeRandomDuration) init(baseDuration time.Duration, minValue int, maxValue int) RangeRandomDuration {
	s.baseDuration = baseDuration
	s.SetRange(minValue, maxValue)
	return s
}

func (s *rangeRandomDuration) Get() time.Duration {
	if s.baseDuration <= 0 || s.minValue <= 0 || s.maxValue <= 0 {
		return time.Duration(0)
	}
	return s.baseDuration * time.Duration(s.minValue+rand.IntN(s.maxValue-s.minValue+1))
}

func (s *rangeRandomDuration) GetBaseDuration() time.Duration {
	return s.baseDuration
}

func (s *rangeRandomDuration) SetBaseDuration(baseDuration time.Duration) RangeRandomDuration {
	s.baseDuration = baseDuration
	return s
}

func (s *rangeRandomDuration) GetRange() (int, int) {
	return s.minValue, s.maxValue
}

func (s *rangeRandomDuration) SetRange(minValue int, maxValue int) RangeRandomDuration {
	if maxValue < minValue {
		minValue, maxValue = maxValue, minValue
	}
	s.minValue, s.maxValue = minValue, maxValue
	return s
}

// CacheQuery Use cache to query data.
type CacheQuery interface {
	// Cache Get Cache.
	Cache() *Cache

	// MultiMutex Get MultiMutex.
	MultiMutex() MultiMutex

	// CacheKey Using custom method to build cache key.
	CacheKey(cacheKey func(maker Maker) (string, error)) CacheQuery

	// RangeRandomDuration Get a random Duration between minValue*baseDuration and maxValue*baseDuration.
	RangeRandomDuration(baseDuration time.Duration, minValue int, maxValue int) time.Duration

	// Del Delete data from cache by maker.
	Del(ctx context.Context, maker Maker) error

	// Has Is there data in the cache?
	Has(ctx context.Context, maker Maker) (bool, error)

	// Get First, query the cache for data; if the data is not found in the cache, then query the database.
	// It is strongly recommended to use slicing to cache data to avoid getting an ErrNoRows error when querying a single data record.
	Get(ctx context.Context, maker Maker, data any, duration time.Duration) error
}

func NewCacheQuery(cache *Cache, multiMutex MultiMutex, way *Way) CacheQuery {
	if cache == nil || multiMutex == nil || way == nil {
		panic(errors.New("hey: illegal parameter value"))
	}
	return &cacheQuery{
		cache:      cache,
		multiMutex: multiMutex,
		way:        way,
	}
}

type cacheQuery struct {
	cache *Cache

	multiMutex MultiMutex

	way *Way

	cacheKey func(maker Maker) (string, error)
}

func (s *cacheQuery) Cache() *Cache {
	return s.cache
}

func (s *cacheQuery) MultiMutex() MultiMutex {
	return s.multiMutex
}

func (s *cacheQuery) CacheKey(cacheKey func(maker Maker) (string, error)) CacheQuery {
	if cacheKey != nil {
		s.cacheKey = cacheKey
	}
	return s
}

func (s *cacheQuery) RangeRandomDuration(baseDuration time.Duration, minValue int, maxValue int) time.Duration {
	return s.cache.RangeRandomDuration(baseDuration, minValue, maxValue)
}

func (s *cacheQuery) newCacheMaker(script Maker) CacheMaker {
	cache := s.cache.Maker(script)
	if s.cacheKey != nil {
		cache.UseCacheKey(s.cacheKey)
	}
	return cache
}

func (s *cacheQuery) Del(ctx context.Context, maker Maker) error {
	return s.newCacheMaker(maker).Del(ctx)
}

func (s *cacheQuery) Has(ctx context.Context, maker Maker) (bool, error) {
	return s.newCacheMaker(maker).Has(ctx)
}

func (s *cacheQuery) get(
	ctx context.Context,
	maker Maker,
	duration time.Duration,
	query func(ctx context.Context, script *SQL) error,
	cacheGet func(ctx context.Context, cache CacheMaker) error,
	cacheSet func(ctx context.Context, cache CacheMaker, duration time.Duration) error,
) error {
	if maker == nil {
		return ErrInvalidMaker
	}
	script := maker.ToSQL()
	if script == nil || script.IsEmpty() {
		return ErrEmptySqlStatement
	}
	// No caching is used in transactions.
	if s.way.IsInTransaction() {
		return query(ctx, script)
	}
	// Query cached data.
	cache := s.newCacheMaker(script)
	err := cacheGet(ctx, cache)
	if err != nil {
		if !errors.Is(err, ErrNoDataInCache) {
			return err
		}
	} else {
		return nil
	}
	// Get the key corresponding to the cached data.
	key, err := cache.GetCacheKey()
	if err != nil {
		return err
	}
	// Acquire the mutex using the cache key.
	mutex := s.multiMutex.Get(key)
	mutex.Lock()
	defer mutex.Unlock()
	// Check the cache again to see if there is any data.
	err = cacheGet(ctx, cache)
	if err != nil {
		if !errors.Is(err, ErrNoDataInCache) {
			return err
		}
	} else {
		return nil
	}

	// Query data from the database.
	err = query(ctx, script)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Cache even if there is no data in the database.
			result := cacheSet(ctx, cache, duration)
			if result != nil {
				return result
			}
			return err
		}
		return err
	}
	// Cache query data.
	return cacheSet(ctx, cache, duration)
}

func (s *cacheQuery) Get(ctx context.Context, maker Maker, data any, duration time.Duration) error {
	return s.get(
		ctx,
		maker,
		duration,
		func(ctx context.Context, script *SQL) error {
			return s.way.Scan(ctx, script, data)
		},
		func(ctx context.Context, cache CacheMaker) error {
			return cache.GetUnmarshal(ctx, data)
		},
		func(ctx context.Context, cache CacheMaker, duration time.Duration) error {
			return cache.MarshalSet(ctx, data, duration)
		},
	)
}
