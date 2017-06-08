package store

import (
	"github.com/go-redis/redis"
)

const Nil = redis.Nil

type Driver interface {
	GetSysRecord(key string) (string, error)
	SetSysRecord(key string, value string, ttl int64) error
	DelSysRecord(key string) error
	ScanSysRecords(cursor uint64, match string, count int64) ([]string, uint64, error)
	GetUserRecord(key string) (string, error)
	SetUserRecord(key string, value string, ttl int64) error
	DelUserRecord(key string) error
	ScanUserRecords(cursor uint64, match string, count int64) ([]string, uint64, error)
}
