// Cache query data to reduce database pressure; Used in combination with query functions to reduce coupling.
// Supports fast extraction of specific types of data from the cache, including int, string, float, bool ...
// You are not restricted to using a specific cache or data serialization method, and JSON serialization and deserialization are used by default.

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

// Cache Read data from the cache and write data to the cache.
type Cache struct {
	getter    func(key string) (value []byte, exists bool, err error)
	setter    func(key string, value []byte, duration ...time.Duration) error
	deleter   func(key string) error
	exists    func(key string) (exists bool, err error)
	key       func(key string) string
	marshal   func(v interface{}) ([]byte, error)
	unmarshal func(data []byte, v interface{}) error
}

func (s *Cache) UseGetter(getter func(key string) (value []byte, exists bool, err error)) *Cache {
	if s.getter != nil {
		s.getter = getter
	}
	return s
}

func (s *Cache) UseSetter(setter func(key string, value []byte, duration ...time.Duration) error) *Cache {
	if s.setter != nil {
		s.setter = setter
	}
	return s
}

func (s *Cache) UseDeleter(deleter func(key string) error) *Cache {
	if s.deleter != nil {
		s.deleter = deleter
	}
	return s
}

func (s *Cache) UseExists(exists func(key string) (exists bool, err error)) *Cache {
	if exists != nil {
		s.exists = exists
	}
	return s
}

func (s *Cache) UseKey(key func(key string) string) *Cache {
	if key != nil {
		s.key = key
	}
	return s
}

func (s *Cache) UseMarshal(marshal func(v interface{}) ([]byte, error)) *Cache {
	if marshal != nil {
		s.marshal = marshal
	}
	return s
}

func (s *Cache) UseUnmarshal(unmarshal func(data []byte, v interface{}) error) *Cache {
	if unmarshal != nil {
		s.unmarshal = unmarshal
	}
	return s
}

func (s *Cache) Marshal() func(v interface{}) ([]byte, error) {
	return s.marshal
}

func (s *Cache) Unmarshal() func(data []byte, v interface{}) error {
	return s.unmarshal
}

func (s *Cache) Get(key string) (value []byte, exists bool, err error) {
	if s.key != nil {
		key = s.key(key)
	}
	return s.getter(key)
}

func (s *Cache) Set(key string, value []byte, duration ...time.Duration) error {
	if s.key != nil {
		key = s.key(key)
	}
	return s.setter(key, value, duration...)
}

func (s *Cache) Del(key string) error {
	return s.deleter(key)
}

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

func (s *Cache) SetMarshal(key string, value interface{}, duration ...time.Duration) error {
	tmp, err := s.marshal(value)
	if err != nil {
		return err
	}
	return s.Set(key, tmp, duration...)
}

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

func (s *Cache) SetString(key string, value string, duration ...time.Duration) error {
	return s.Set(key, []byte(value), duration...)
}

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

func (s *Cache) SetInt(key string, value int64, duration ...time.Duration) error {
	return s.Set(key, []byte(strconv.FormatInt(value, 10)), duration...)
}

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

func (s *Cache) SetFloat(key string, value float64, duration ...time.Duration) error {
	return s.Set(key, []byte(strconv.FormatFloat(value, 'f', -1, 64)), duration...)
}

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

func (s *Cache) SetBool(key string, value bool, duration ...time.Duration) error {
	return s.Set(key, fmt.Appendf(nil, "%t", value), duration...)
}

func (s *Cache) Exists(key string) (exists bool, err error) {
	if s.exists != nil {
		exists, err = s.exists(key)
	} else {
		_, exists, err = s.getter(key)
	}
	return
}

func (s *Cache) RandDuration(min int, max int, duration time.Duration) time.Duration {
	return time.Duration(min+rand.IntN(max-min+1)) * duration
}

func (s *Cache) Fork() *Cache {
	forkCache := *s
	return &forkCache
}

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

// CacheQuery Get query results from the cache and store query results in the cache.
type CacheQuery struct {
	cache *Cache
	get   *Get

	prepare string
	args    []interface{}
	key     string
}

// buildCacheKey Use prepare and args to calculate the hash value as the cache key.
func (s *CacheQuery) buildCacheKey() error {
	if s.key != EmptyString {
		return nil
	}

	s.prepare, s.args = s.get.Cmd()
	if s.prepare == EmptyString {
		return errors.New("the query statement is empty")
	}

	for index, value := range s.args {
		if tmp, ok := value.([]byte); ok && tmp != nil {
			s.args[index] = hex.EncodeToString(tmp)
		}
	}

	param, err := s.cache.Marshal()(s.args)
	if err != nil {
		return err
	}

	buffer := bytes.NewBufferString(s.prepare)
	if _, err = buffer.WriteString(";"); err != nil {
		return err
	}
	if _, err = buffer.Write(param); err != nil {
		return err
	}

	hash := sha256.New()
	if _, err = hash.Write(buffer.Bytes()); err != nil {
		return err
	}
	s.key = hex.EncodeToString(hash.Sum(nil))

	return nil
}

func (s *CacheQuery) Get() (value []byte, exists bool, err error) {
	if err = s.buildCacheKey(); err != nil {
		return nil, false, err
	}
	return s.cache.Get(s.key)
}

func (s *CacheQuery) Set(value []byte, duration ...time.Duration) error {
	if err := s.buildCacheKey(); err != nil {
		return err
	}
	return s.cache.Set(s.key, value, duration...)
}

func (s *CacheQuery) Del() error {
	if err := s.buildCacheKey(); err != nil {
		return err
	}
	return s.cache.Del(s.key)
}

func (s *CacheQuery) GetUnmarshal(value interface{}) (exists bool, err error) {
	if err = s.buildCacheKey(); err != nil {
		return false, err
	}
	return s.cache.GetUnmarshal(s.key, value)
}

func (s *CacheQuery) SetMarshal(value interface{}, duration ...time.Duration) error {
	if err := s.buildCacheKey(); err != nil {
		return err
	}
	return s.cache.SetMarshal(s.key, value, duration...)
}

func (s *CacheQuery) Exists() (exists bool, err error) {
	if err = s.buildCacheKey(); err != nil {
		return false, err
	}
	return s.cache.Exists(s.key)
}

func (s *CacheQuery) GetString() (string, bool, error) {
	if err := s.buildCacheKey(); err != nil {
		return EmptyString, false, err
	}
	return s.cache.GetString(s.key)
}

func (s *CacheQuery) GetInt() (int64, bool, error) {
	if err := s.buildCacheKey(); err != nil {
		return 0, false, err
	}
	return s.cache.GetInt(s.key)
}

func (s *CacheQuery) GetFloat() (float64, bool, error) {
	if err := s.buildCacheKey(); err != nil {
		return 0, false, err
	}
	return s.cache.GetFloat(s.key)
}

func (s *CacheQuery) GetBool() (bool, bool, error) {
	if err := s.buildCacheKey(); err != nil {
		return false, false, err
	}
	return s.cache.GetBool(s.key)
}

func (s *CacheQuery) ResetGet(get *Get) *CacheQuery {
	if get != nil {
		s.prepare, s.args, s.key, s.get = EmptyString, nil, EmptyString, get
	}
	return s
}

func NewCacheQuery(cache *Cache, get *Get) *CacheQuery {
	if cache == nil || get == nil {
		return nil
	}
	return &CacheQuery{
		cache: cache,
		get:   get,
	}
}
