package database

import (
	"context"
	"fmt"

	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()
var RedisIdDb *redis.Client
var RedisStatsDb *redis.Client

func MustConnectToRedis(config *conf.Conf) {
	if config == nil {
		panic("config is nil")
	}
	if config.RedisHost == "" || config.RedisPort == "" {
		panic("Redis host is empty")
	}

	rdUuidDb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", config.RedisHost, config.RedisPort),
		Password: config.RedisPassword,
		DB:       config.RedisUuidDB,
	})
	_, err := rdUuidDb.Ping(ctx).Result()
	if err != nil {
		panic(err)
	}
	RedisIdDb = rdUuidDb

	rdStreamStatsDb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", config.RedisHost, config.RedisPort),
		Password: config.RedisPassword,
		DB:       config.RedisConnidsDB,
	})
	_, err = rdStreamStatsDb.Ping(ctx).Result()
	if err != nil {
		panic(err)
	}
	RedisStatsDb = rdStreamStatsDb
}
