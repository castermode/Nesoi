package store

import (
	"time"

	"github.com/go-redis/redis"
)

type RedisDriver struct {
	client *redis.Client
}

func NewRedisDriver(redisAddr string) (*RedisDriver, error) {
	c := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	if _, err := c.Ping().Result(); err != nil {
		return nil, err
	}

	return &RedisDriver{client: c}, nil
}

func (rd *RedisDriver) GetUserRecord(key string) (string, error) {
	value, err := rd.client.Get(key).Result()
	if err == redis.Nil {
		return value, Nil
	}
	return value, err
}

func (rd *RedisDriver) SetUserRecord(key string, value string, ttl int64) error {
	return rd.client.Set(key, value, time.Duration(ttl)).Err()
}

func (rd *RedisDriver) DelUserRecord(key string) error {
	_, err := rd.client.Del(key).Result()
	return err
}

func (rd *RedisDriver) ScanUserRecords(cursor uint64, match string, count int64) ([]string, uint64, error) {
	return rd.client.Scan(cursor, match, count).Result()
}

func (rd *RedisDriver) GetSysRecord(key string) (string, error) {
	return rd.GetUserRecord(key)
}

func (rd *RedisDriver) SetSysRecord(key string, value string, ttl int64) error {
	return rd.SetUserRecord(key, value, ttl)
}

func (rd *RedisDriver) DelSysRecord(key string) error {
	return rd.DelUserRecord(key)
}

func (rd *RedisDriver) ScanSysRecords(cursor uint64, match string, count int64) ([]string, uint64, error) {
	return rd.ScanUserRecords(cursor, match, count)
}
