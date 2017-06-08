package store

import (
	"time"

	"github.com/go-redis/redis"
)

type DistkvDriver struct {
	sysClient  *redis.Client
	userClient *redis.Client
}

func NewDistkvDriver(sysAddr string, userAddr string) (*DistkvDriver, error) {
	sc := redis.NewClient(&redis.Options{
		Addr:     sysAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	if _, err := sc.Ping().Result(); err != nil {
		return nil, err
	}

	uc := redis.NewClient(&redis.Options{
		Addr:     userAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	if _, err := uc.Ping().Result(); err != nil {
		return nil, err
	}

	return &DistkvDriver{sysClient: sc, userClient: uc}, nil
}

func (dd *DistkvDriver) GetUserRecord(key string) (string, error) {
	value, err := dd.userClient.Get(key).Result()
	if err == redis.Nil {
		return value, Nil
	}

	return value, err
}

func (dd *DistkvDriver) SetUserRecord(key string, value string, ttl int64) error {
	return dd.userClient.Set(key, value, time.Duration(ttl)).Err()
}

func (dd *DistkvDriver) DelUserRecord(key string) error {
	_, err := dd.userClient.Del(key).Result()
	return err
}

func (dd *DistkvDriver) ScanUserRecords(cursor uint64, match string, count int64) ([]string, uint64, error) {
	return dd.userClient.Scan(cursor, match, count).Result()
}

func (dd *DistkvDriver) GetSysRecord(key string) (string, error) {
	value, err := dd.sysClient.Get(key).Result()
	if err == redis.Nil {
		return value, Nil
	}

	return value, err
}

func (dd *DistkvDriver) SetSysRecord(key string, value string, ttl int64) error {
	return dd.sysClient.Set(key, value, time.Duration(ttl)).Err()
}

func (dd *DistkvDriver) DelSysRecord(key string) error {
	_, err := dd.sysClient.Del(key).Result()
	return err
}

func (dd *DistkvDriver) ScanSysRecords(cursor uint64, match string, count int64) ([]string, uint64, error) {
	return dd.sysClient.Scan(cursor, match, count).Result()
}
