package cache

import (
	"context"
	"errors"
	"pulseguard/services/ingestion/internal/config"
	"strconv"

	"github.com/redis/go-redis/v9"
)

var (
	ErrConnectionCancel        = errors.New("obtained client canceled connection")
	ErrUndefinedKey            = errors.New("obtained key is undefined")
	ErrUnknownTypeOfProjectKey = errors.New("redis contains unknown type of project key")
	ErrRedis                   = errors.New("error from redis")
)

type RedisProjectCache struct {
	redis *redis.Client
}

func NewRedisProjectCache(ctx context.Context, cfg config.RedisConfig) (*RedisProjectCache, error) {
	redis := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Pass,
		DB:       cfg.DB,
		PoolSize: cfg.PoolSize,
	})

	err := redis.Ping(ctx).Err()
	if err != nil {
		return nil, ErrConnectionCancel
	}

	return &RedisProjectCache{
		redis: redis,
	}, nil
}

func (rc RedisProjectCache) CheckKey(ctx context.Context, key string) (int, error) {
	sv, err := rc.redis.Get(ctx, key).Result()
	if err != nil {
		switch err {
		case redis.Nil:
			return 0, ErrUndefinedKey
		default:
			return 0, ErrRedis
		}
	}

	iv, err := strconv.Atoi(sv)
	if err != nil {
		return 0, ErrUnknownTypeOfProjectKey
	}

	return iv, nil
}

func (rc RedisProjectCache) Close() error {
	return rc.redis.Close()
}
