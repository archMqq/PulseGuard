package cache

import (
	"context"
	"errors"
	"pulseguard/services/pkg/buffers"
	"pulseguard/services/pkg/logger"
	"pulseguard/services/processing/internal/config"
	"sync"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var (
	ErrConnectionCancel   = errors.New("obtained client canceled connection")
	ErrUnknownFingerpring = errors.New("got new fingerprint")
)

type RedisCache struct {
	redis      *redis.Client
	bufferPool *sync.Pool
	log        logger.Logger
}

func NewRedisProjectCache(ctx context.Context, cfg config.RedisConfig,
	pool *sync.Pool, log logger.Logger) (*RedisCache, error) {
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
		redis:      redis,
		bufferPool: pool,
		log:        log,
	}, nil
}

func (rc RedisCache) CheckEvent(ctx context.Context, fp uint64) (uuid.UUID, error) {
	buffer := rc.bufferPool.Get().(buffers.NumBuffer)
	fpStr := buffer.Convert(fp)
	id, err := rc.redis.Get(ctx, fpStr).Result()
	if err != nil {
		switch err {
		case redis.Nil:
			return uuid.UUID{}, ErrUnknownFingerpring
		default:
			rc.log.Error("unknown redis err", err)
			return uuid.UUID{}, err
		}
	}

	uu, err := uuid.Parse(id)
	if err != nil {
		rc.log.Error("unknown fingerprint id type")
		return uuid.UUID{}, err
	}

	return uu, nil
}
