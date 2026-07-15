package cache

import (
	"context"
	"errors"
	"pulseguard/services/pkg/logger"
	"pulseguard/services/processing/internal/config"

	"github.com/redis/go-redis/v9"
)

var (
	ErrConnectionCancel = errors.New("obtained client canceled connection")
)

type RedisCache struct {
	redis *redis.Client
	log   logger.Logger
}

func NewRedisProjectCache(ctx context.Context, cfg config.RedisConfig, log logger.Logger) (*RedisCache, error) {
	redis := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Pass,
		DB:       cfg.DB,
		PoolSize: cfg.PoolSize,
	})

	if err := redis.Ping(ctx); err.Err() != nil {
		log.Error(ErrConnectionCancel.Error(), err.Err())
		return nil, ErrConnectionCancel
	}

	return &RedisCache{
		redis: redis,
		log:   log,
	}, nil
}

func (rc RedisCache) Process(ctx context.Context) {

}
