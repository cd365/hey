// Cache query data to reduce database pressure; Used in combination with query functions to reduce coupling.
// Supports fast extraction of specific types of data from the cache, including int, string, float, bool ...
// You are not restricted to using a specific cache or data serialization method, and JSON serialization and deserialization are used by default.
// You can rewrite the cache key according to your business needs, such as adding a specific prefix.

package hey

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"
)

// Cache Read and write data in cache.
type Cache struct {
	// getter Reading data from the cache.
	getter func(key string) (value []byte, exists bool, err error)

	// setter Writing data to cache.
	setter func(key string, value []byte, duration ...time.Duration) error

	// deleter Deleting data from the cache.
	deleter func(key string) error

	// exists Whether cached data exists?
	exists func(key string) (exists bool, err error)

	// key Customize cache key processing, such as adding a specified prefix.
	key func(key string) string

	// marshal Serializing cache data.
	marshal func(v interface{}) ([]byte, error)

	// unmarshal Deserializing cache data.
	unmarshal func(data []byte, v interface{}) error
}

// UseGetter Customize the read data from the cache.
func (s *Cache) UseGetter(getter func(key string) (value []byte, exists bool, err error)) *Cache {
	if s.getter != nil {
		s.getter = getter
	}
	return s
}

// UseSetter Customize the write data to cache.
func (s *Cache) UseSetter(setter func(key string, value []byte, duration ...time.Duration) error) *Cache {
	if s.setter != nil {
		s.setter = setter
	}
	return s
}

// UseDeleter Customize whether cached data exists.
func (s *Cache) UseDeleter(deleter func(key string) error) *Cache {
	if s.deleter != nil {
		s.deleter = deleter
	}
	return s
}

// UseExists Customize whether the data exists in the cache.
func (s *Cache) UseExists(exists func(key string) (exists bool, err error)) *Cache {
	if exists != nil {
		s.exists = exists
	}
	return s
}

// UseKey Customize cache key processing, such as adding a specified prefix.
func (s *Cache) UseKey(key func(key string) string) *Cache {
	if key != nil {
		s.key = key
	}
	return s
}

// UseMarshal Custom serializing cache data.
func (s *Cache) UseMarshal(marshal func(v interface{}) ([]byte, error)) *Cache {
	if marshal != nil {
		s.marshal = marshal
	}
	return s
}

// UseUnmarshal Custom deserializing cache data.
func (s *Cache) UseUnmarshal(unmarshal func(data []byte, v interface{}) error) *Cache {
	if unmarshal != nil {
		s.unmarshal = unmarshal
	}
	return s
}

// Key Get custom method of key.
func (s *Cache) Key() func(key string) string {
	return s.key
}

// Marshal Get custom method of marshal.
func (s *Cache) Marshal() func(v interface{}) ([]byte, error) {
	return s.marshal
}

// Unmarshal Get custom method of unmarshal.
func (s *Cache) Unmarshal() func(data []byte, v interface{}) error {
	return s.unmarshal
}

// Get Read cache data from cache.
func (s *Cache) Get(key string) (value []byte, exists bool, err error) {
	if s.key != nil {
		key = s.key(key)
	}
	return s.getter(key)
}

// Set Write cache data to cache.
func (s *Cache) Set(key string, value []byte, duration ...time.Duration) error {
	if s.key != nil {
		key = s.key(key)
	}
	return s.setter(key, value, duration...)
}

// Del Deleting data from the cache.
func (s *Cache) Del(key string) error {
	return s.deleter(key)
}

// Exists Whether cached data exists?
func (s *Cache) Exists(key string) (exists bool, err error) {
	if had := s.exists; had != nil {
		exists, err = had(key)
	} else {
		_, exists, err = s.getter(key)
	}
	return
}

// GetUnmarshal Read cached data from the cache and deserialize cached data.
func (s *Cache) GetUnmarshal(key string, value interface{}) (exists bool, err error) {
	tmp, exists, err := s.Get(key)
	if err != nil {
		return exists, err
	}
	if !exists {
		return exists, nil
	}
	if err = s.unmarshal(tmp, value); err != nil {
		return exists, err
	}
	return exists, nil
}

// MarshalSet Serialize cache data and write the serialized data to the cache.
func (s *Cache) MarshalSet(key string, value interface{}, duration ...time.Duration) error {
	tmp, err := s.marshal(value)
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

// RandDuration Get a random Duration between min*duration and max*duration.
func (s *Cache) RandDuration(min int, max int, duration time.Duration) time.Duration {
	return time.Duration(min+rand.IntN(max-min+1)) * duration
}

// Fork Copy the current object.
func (s *Cache) Fork() *Cache {
	forkCache := *s
	return &forkCache
}

// NewCache Create a new *Cache object.
func NewCache(
	getter func(key string) (value []byte, exists bool, err error), // nil value are not allowed
	setter func(key string, value []byte, duration ...time.Duration) error, // nil value are not allowed
	deleter func(key string) error, // nil value are not allowed
) *Cache {
	return &Cache{
		getter:    getter,
		setter:    setter,
		deleter:   deleter,
		marshal:   json.Marshal,
		unmarshal: json.Unmarshal,
	}
}

// CacheCmder Cache SQL statement related data, including but not limited to cache query data.
type CacheCmder interface {
	// GetCacheKey Use prepare and args to calculate the hash value as the cache key.
	GetCacheKey() (string, error)

	// UseCacheKey Custom build cache key.
	UseCacheKey(cacheKey func(cmder Cmder) (string, error)) CacheCmder

	// Reset For reset cmder and it's related property values.
	Reset(cmder ...Cmder) CacheCmder

	// Get For get value from cache.
	Get() (value []byte, exists bool, err error)

	// Set For set value to cache.
	Set(value []byte, duration ...time.Duration) error

	// Del Delete data in the cache based on cache key.
	Del() error

	// Exists Check whether the cache key exists.
	Exists() (exists bool, err error)

	// GetUnmarshal Query data and unmarshal data.
	GetUnmarshal(value interface{}) (exists bool, err error)

	// MarshalSet Marshal data and set data.
	MarshalSet(value interface{}, duration ...time.Duration) error

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

// cacheCmd Implementing the CacheCmder interface.
type cacheCmd struct {
	cache *Cache
	cmder Cmder

	cacheKey func() (string, error)

	prepare string
	args    []interface{}
	key     string
}

func (s *cacheCmd) getCacheKey() (string, error) {
	if s.key != EmptyString {
		return s.key, nil
	}

	s.prepare, s.args = s.cmder.Cmd()
	if s.prepare == EmptyString {
		return EmptyString, errors.New("the prepare value is empty")
	}

	for index, value := range s.args {
		if tmp, ok := value.([]byte); ok && tmp != nil {
			s.args[index] = hex.EncodeToString(tmp)
		}
	}

	param, err := s.cache.Marshal()(s.args)
	if err != nil {
		return EmptyString, err
	}

	buffer := bytes.NewBufferString(s.prepare)
	if _, err = buffer.WriteString(";"); err != nil {
		return EmptyString, err
	}
	if _, err = buffer.Write(param); err != nil {
		return EmptyString, err
	}

	hash := sha256.New()
	if _, err = hash.Write(buffer.Bytes()); err != nil {
		return EmptyString, err
	}
	s.key = hex.EncodeToString(hash.Sum(nil))

	return s.key, nil
}

func (s *cacheCmd) GetCacheKey() (string, error) {
	if cacheKey := s.cacheKey; cacheKey != nil {
		return cacheKey()
	}
	return s.getCacheKey()
}

func (s *cacheCmd) UseCacheKey(cacheKey func(cmder Cmder) (string, error)) CacheCmder {
	if cacheKey != nil {
		s.cacheKey = func() (string, error) { return cacheKey(s.cmder) }
	}
	return s
}

func (s *cacheCmd) Reset(cmder ...Cmder) CacheCmder {
	s.prepare, s.args, s.key = EmptyString, nil, EmptyString
	for _, tmp := range cmder {
		if tmp != nil {
			s.cmder = tmp
			break
		}
	}
	return s
}

func (s *cacheCmd) Get() (value []byte, exists bool, err error) {
	if _, err = s.GetCacheKey(); err != nil {
		return nil, false, err
	}
	return s.cache.Get(s.key)
}

func (s *cacheCmd) Set(value []byte, duration ...time.Duration) error {
	if _, err := s.GetCacheKey(); err != nil {
		return err
	}
	return s.cache.Set(s.key, value, duration...)
}

func (s *cacheCmd) Del() error {
	if _, err := s.GetCacheKey(); err != nil {
		return err
	}
	return s.cache.Del(s.key)
}

func (s *cacheCmd) Exists() (exists bool, err error) {
	if _, err = s.GetCacheKey(); err != nil {
		return false, err
	}
	return s.cache.Exists(s.key)
}

func (s *cacheCmd) GetUnmarshal(value interface{}) (exists bool, err error) {
	if _, err = s.GetCacheKey(); err != nil {
		return false, err
	}
	return s.cache.GetUnmarshal(s.key, value)
}

func (s *cacheCmd) MarshalSet(value interface{}, duration ...time.Duration) error {
	if _, err := s.GetCacheKey(); err != nil {
		return err
	}
	return s.cache.MarshalSet(s.key, value, duration...)
}

func (s *cacheCmd) GetString() (string, bool, error) {
	if _, err := s.GetCacheKey(); err != nil {
		return EmptyString, false, err
	}
	return s.cache.GetString(s.key)
}

func (s *cacheCmd) SetString(value string, duration ...time.Duration) error {
	return s.cache.SetString(s.key, value, duration...)
}

func (s *cacheCmd) GetFloat() (float64, bool, error) {
	if _, err := s.GetCacheKey(); err != nil {
		return 0, false, err
	}
	return s.cache.GetFloat(s.key)
}

func (s *cacheCmd) SetFloat(value float64, duration ...time.Duration) error {
	return s.cache.SetFloat(s.key, value, duration...)
}

func (s *cacheCmd) GetInt() (int64, bool, error) {
	if _, err := s.GetCacheKey(); err != nil {
		return 0, false, err
	}
	return s.cache.GetInt(s.key)
}

func (s *cacheCmd) SetInt(value int64, duration ...time.Duration) error {
	return s.cache.SetInt(s.key, value, duration...)
}

func (s *cacheCmd) GetBool() (bool, bool, error) {
	if _, err := s.GetCacheKey(); err != nil {
		return false, false, err
	}
	return s.cache.GetBool(s.key)
}

func (s *cacheCmd) SetBool(value bool, duration ...time.Duration) error {
	return s.cache.SetBool(s.key, value, duration...)
}

// NewCacheCmder Create a new CacheCmder object.
func NewCacheCmder(cache *Cache, cmder Cmder) CacheCmder {
	if cache == nil || cmder == nil {
		return nil
	}
	return &cacheCmd{
		cache: cache,
		cmder: cmder,
	}
}
