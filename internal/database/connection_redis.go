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

	rdStreamIdDb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", config.RedisHost, config.RedisPort),
		Password: config.RedisPassword,
		DB:       0,
	})
	_, err := rdStreamIdDb.Ping(ctx).Result()
	if err != nil {
		panic(err)
	}
	RedisIdDb = rdStreamIdDb


	rdStreamStatsDb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", config.RedisHost, config.RedisPort),
		Password: config.RedisPassword,
		DB:       1,
	})
	_, err = rdStreamStatsDb.Ping(ctx).Result()
	if err != nil {
		panic(err)
	}
	RedisStatsDb = rdStreamStatsDb
}
