package hey

import (
	"testing"
	"time"
)

func TestNewCacheMaker(t *testing.T) {
	way := testWay()
	if way.GetDatabase() != nil {
		data, err := usingCache(way, nil, NewStringMutex(256))
		if err != nil {
			t.Error(err.Error())
		} else {
			t.Log(data)
		}
	}
}

type ExampleAnyStruct struct {
	Name string `db:"name"` // Table column name
	Age  int    `db:"age"`  // Table column age
}

// usingCache Using cache query data.
func usingCache(way *Way, cacher Cacher, mutexes *StringMutex) (data []*ExampleAnyStruct, err error) {
	// The cacher is usually implemented in Memcached, Redis, current program memory, or even files.
	cache := NewCache(cacher)

	get := way.Get("your_table_name").Select("name", "age").Desc("id").Limit(20).Offset(0)

	cacheGet := NewCacheMaker(cache, get)

	data = make([]*ExampleAnyStruct, 0)

	exists, err := cacheGet.GetUnmarshal(&data)
	if err != nil {
		return nil, err
	}

	if exists {
		// The data has been obtained from the cache and no longer needs to be queried from the database.
		return data, nil
	}

	// Prepare to query data from the database.
	cacheKey, _ := cacheGet.GetCacheKey()

	mutex := mutexes.Get(cacheKey)

	mutex.Lock()
	defer mutex.Unlock()

	exists, err = cacheGet.GetUnmarshal(&data)
	if err != nil {
		return nil, err
	}

	if exists {
		// The data has been obtained from the cache and no longer needs to be queried from the database.
		return data, nil
	}

	// Querying data from database.
	if err = get.Get(&data); err != nil {
		return nil, err
	}

	// Cache query data.
	if err = cacheGet.MarshalSet(data, cache.DurationRange(time.Second, 7, 9)); err != nil {
		return nil, err
	}

	return data, nil
}
