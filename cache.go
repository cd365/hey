// Cache query data to reduce database pressure; Used in combination with query functions to reduce coupling.
// Supports fast extraction of specific types of data from the cache, including bool, int, float, string ...

package hey

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"
)

// Cacher Objects that implement cache.
type Cacher interface {
	// Key Customize cache key processing before reading and writing cache.
	Key(key string) string

	// Get Reading data from the cache.
	Get(key string) (value []byte, exists bool, err error)

	// Set Writing data to the cache.
	Set(key string, value []byte, duration ...time.Duration) error

	// Del Deleting data from the cache.
	Del(key string) error

	// Exists Check if a certain data exists in the cache.
	Exists(key string) (exists bool, err error)

	// Marshal Serialize cache data.
	Marshal(v any) ([]byte, error)

	// Unmarshal Deserialize cache data.
	Unmarshal(data []byte, v any) error
}

// Cache Read and write data in cache.
type Cache struct {
	cacher Cacher
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
func (s *Cache) Get(key string) (value []byte, exists bool, err error) {
	return s.cacher.Get(s.cacher.Key(key))
}

// Set Write cache data to cache.
func (s *Cache) Set(key string, value []byte, duration ...time.Duration) error {
	return s.cacher.Set(s.cacher.Key(key), value, duration...)
}

// Del Deleting data from the cache.
func (s *Cache) Del(key string) error {
	return s.cacher.Del(s.cacher.Key(key))
}

// Exists Whether cached data exists?
func (s *Cache) Exists(key string) (exists bool, err error) {
	return s.cacher.Exists(s.cacher.Key(key))
}

// GetUnmarshal Read cached data from the cache and deserialize cached data.
func (s *Cache) GetUnmarshal(key string, value any) (exists bool, err error) {
	tmp, exists, err := s.Get(key)
	if err != nil {
		return exists, err
	}
	if !exists {
		return exists, nil
	}
	if err = s.cacher.Unmarshal(tmp, value); err != nil {
		return exists, err
	}
	return exists, nil
}

// MarshalSet Serialize cache data and write the serialized data to the cache.
func (s *Cache) MarshalSet(key string, value any, duration ...time.Duration) error {
	tmp, err := s.cacher.Marshal(value)
	if err != nil {
		return err
	}
	return s.Set(key, tmp, duration...)
}

// GetString Read string cache data from cache.
func (s *Cache) GetString(key string) (value string, exists bool, err error) {
	result, exists, err := s.Get(key)
	if err != nil {
		return EmptyString, exists, err
	}
	if !exists {
		return EmptyString, exists, nil
	}
	return string(result), exists, nil
}

// SetString Write string cache data to cache.
func (s *Cache) SetString(key string, value string, duration ...time.Duration) error {
	return s.Set(key, []byte(value), duration...)
}

// GetFloat Read float64 cache data from cache.
func (s *Cache) GetFloat(key string) (value float64, exists bool, err error) {
	result, exists, err := s.Get(key)
	if err != nil {
		return 0, exists, err
	}
	if !exists {
		return 0, exists, nil
	}
	f64, err := strconv.ParseFloat(string(result), 64)
	if err != nil {
		return 0, exists, err
	}
	return f64, exists, nil
}

// SetFloat Write float64 cache data to cache.
func (s *Cache) SetFloat(key string, value float64, duration ...time.Duration) error {
	return s.Set(key, []byte(strconv.FormatFloat(value, 'f', -1, 64)), duration...)
}

// GetInt Read int64 cache data from cache.
func (s *Cache) GetInt(key string) (value int64, exists bool, err error) {
	result, exists, err := s.Get(key)
	if err != nil {
		return 0, exists, err
	}
	if !exists {
		return 0, exists, nil
	}
	i64, err := strconv.ParseInt(string(result), 10, 64)
	if err != nil {
		return 0, exists, err
	}
	return i64, exists, nil
}

// SetInt Write int64 cache data to cache.
func (s *Cache) SetInt(key string, value int64, duration ...time.Duration) error {
	return s.Set(key, []byte(strconv.FormatInt(value, 10)), duration...)
}

// GetBool Read bool cache data from cache.
func (s *Cache) GetBool(key string) (value bool, exists bool, err error) {
	result, exists, err := s.Get(key)
	if err != nil {
		return false, exists, err
	}
	if !exists {
		return false, exists, nil
	}
	boolean, err := strconv.ParseBool(string(result))
	if err != nil {
		return false, exists, err
	}
	return boolean, exists, nil
}

// SetBool Write bool cache data to cache.
func (s *Cache) SetBool(key string, value bool, duration ...time.Duration) error {
	return s.Set(key, fmt.Appendf(nil, "%t", value), duration...)
}

// DurationRange Get a random Duration between minValue*duration and maxValue*duration.
func (s *Cache) DurationRange(duration time.Duration, minValue int, maxValue int) time.Duration {
	return time.Duration(minValue+rand.IntN(maxValue-minValue+1)) * duration
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

// CacheMaker Cache SQL statement related data, including but not limited to cache query data.
type CacheMaker interface {
	// GetCacheKey Use prepare and args to calculate the hash value as the cache key.
	GetCacheKey() (string, error)

	// UseCacheKey Custom build cache key.
	UseCacheKey(cacheKey func(maker Maker) (string, error)) CacheMaker

	// Reset For reset maker and it's related property values.
	Reset(maker ...Maker) CacheMaker

	// Get For get value from cache.
	Get() (value []byte, exists bool, err error)

	// Set For set value to cache.
	Set(value []byte, duration ...time.Duration) error

	// Del Delete data in the cache based on cache key.
	Del() error

	// Exists Check whether the cache key exists.
	Exists() (exists bool, err error)

	// GetUnmarshal Query data and unmarshal data.
	GetUnmarshal(value any) (exists bool, err error)

	// MarshalSet Marshal data and set data.
	MarshalSet(value any, duration ...time.Duration) error

	// GetString Get string type value.
	GetString() (string, bool, error)

	// SetString Set string type value.
	SetString(value string, duration ...time.Duration) error

	// GetFloat Get float64 type value.
	GetFloat() (float64, bool, error)

	// SetFloat Set float64 type value.
	SetFloat(value float64, duration ...time.Duration) error

	// GetInt Get int64 type value.
	GetInt() (int64, bool, error)

	// SetInt Set int64 type value.
	SetInt(value int64, duration ...time.Duration) error

	// GetBool Get boolean type value.
	GetBool() (bool, bool, error)

	// SetBool Set boolean type value.
	SetBool(value bool, duration ...time.Duration) error
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

// getCacheKey Default method for building cache key.
func (s *cacheMaker) getCacheKey() (string, error) {
	if s.key != EmptyString {
		return s.key, nil
	}

	script := s.maker.ToSQL()
	if script == nil || script.Empty() {
		return EmptyString, errors.New("SQL is empty")
	}

	for index, value := range script.Args {
		if tmp, ok := value.([]byte); ok && tmp != nil {
			script.Args[index] = hex.EncodeToString(tmp)
		}
	}
	args, err := s.cache.GetCacher().Marshal(script.Args)
	if err != nil {
		return EmptyString, err
	}

	b := getStringBuilder()
	defer putStringBuilder(b)

	b.WriteString(script.Prepare)
	b.WriteString(";")
	b.Write(args)

	hash := sha256.New()
	if _, err = hash.Write([]byte(b.String())); err != nil {
		return EmptyString, err
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
	if s.key != EmptyString {
		return s.key, nil
	}
	key, err := cacheKey()
	if err != nil {
		return EmptyString, err
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
	s.key = EmptyString
	for _, tmp := range maker {
		if tmp != nil {
			s.maker = tmp
			break
		}
	}
	return s
}

// Get Read data from cache.
func (s *cacheMaker) Get() (value []byte, exists bool, err error) {
	if _, err = s.GetCacheKey(); err != nil {
		return nil, false, err
	}
	return s.cache.Get(s.key)
}

// Set Write data to cache.
func (s *cacheMaker) Set(value []byte, duration ...time.Duration) error {
	if _, err := s.GetCacheKey(); err != nil {
		return err
	}
	return s.cache.Set(s.key, value, duration...)
}

// Del Delete cache value.
func (s *cacheMaker) Del() error {
	if _, err := s.GetCacheKey(); err != nil {
		return err
	}
	return s.cache.Del(s.key)
}

// Exists Check if the cache value exists.
func (s *cacheMaker) Exists() (exists bool, err error) {
	if _, err = s.GetCacheKey(); err != nil {
		return false, err
	}
	return s.cache.Exists(s.key)
}

// GetUnmarshal Get cached value and deserialize.
func (s *cacheMaker) GetUnmarshal(value any) (exists bool, err error) {
	if _, err = s.GetCacheKey(); err != nil {
		return false, err
	}
	return s.cache.GetUnmarshal(s.key, value)
}

// MarshalSet Serialize cache data and set serialized data to the cache.
func (s *cacheMaker) MarshalSet(value any, duration ...time.Duration) error {
	if _, err := s.GetCacheKey(); err != nil {
		return err
	}
	return s.cache.MarshalSet(s.key, value, duration...)
}

// GetString Get string value.
func (s *cacheMaker) GetString() (string, bool, error) {
	if _, err := s.GetCacheKey(); err != nil {
		return EmptyString, false, err
	}
	return s.cache.GetString(s.key)
}

// SetString Set string value.
func (s *cacheMaker) SetString(value string, duration ...time.Duration) error {
	if _, err := s.GetCacheKey(); err != nil {
		return err
	}
	return s.cache.SetString(s.key, value, duration...)
}

// GetFloat Get float64 value.
func (s *cacheMaker) GetFloat() (float64, bool, error) {
	if _, err := s.GetCacheKey(); err != nil {
		return 0, false, err
	}
	return s.cache.GetFloat(s.key)
}

// SetFloat Set float64 value.
func (s *cacheMaker) SetFloat(value float64, duration ...time.Duration) error {
	if _, err := s.GetCacheKey(); err != nil {
		return err
	}
	return s.cache.SetFloat(s.key, value, duration...)
}

// GetInt Get int64 value.
func (s *cacheMaker) GetInt() (int64, bool, error) {
	if _, err := s.GetCacheKey(); err != nil {
		return 0, false, err
	}
	return s.cache.GetInt(s.key)
}

// SetInt Set int64 value.
func (s *cacheMaker) SetInt(value int64, duration ...time.Duration) error {
	if _, err := s.GetCacheKey(); err != nil {
		return err
	}
	return s.cache.SetInt(s.key, value, duration...)
}

// GetBool Get bool value.
func (s *cacheMaker) GetBool() (bool, bool, error) {
	if _, err := s.GetCacheKey(); err != nil {
		return false, false, err
	}
	return s.cache.GetBool(s.key)
}

// SetBool Set bool value.
func (s *cacheMaker) SetBool(value bool, duration ...time.Duration) error {
	if _, err := s.GetCacheKey(); err != nil {
		return err
	}
	return s.cache.SetBool(s.key, value, duration...)
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

// StringMutex maps string keys to a fixed set of sync.Mutex locks using hashing.
type StringMutex struct {
	// length Number of mutexes, fixed after initialization.
	length int

	// mutexes Slice of mutexes, fixed after initialization.
	mutexes []*sync.Mutex
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

type MinMaxDuration struct {
	minValue int // Range minimum value.

	maxValue int // Range maximum value.

	duration time.Duration // Base duration value.
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

// NewMinMaxDuration The minimum value of all values should be granter than 0, unless you want to cache permanently.
func NewMinMaxDuration(duration time.Duration, minValue int, maxValue int) *MinMaxDuration {
	return (&MinMaxDuration{}).init(duration, minValue, maxValue)
}
