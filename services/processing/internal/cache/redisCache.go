package cache

import (
	"context"
	"errors"
	"fmt"
	"pulseguard/services/pkg/buffers"
	"pulseguard/services/pkg/logger"
	"pulseguard/services/processing/internal/config"
	"sync"
	"time"

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

func (rc *RedisCache) CheckIssues(ctx context.Context, fps []uint64) (map[uint64]uuid.UUID, []uint64, error) {
	buffer := rc.bufferPool.Get().(buffers.NumBuffer)
	defer rc.bufferPool.Put(buffer)

	keys := make([]string, len(fps))
	for i, fp := range fps {
		keys[i] = buffer.Convert(fp)
	}

	var res []interface{}
	var err error
	const maxRetries = 3

	for attempt := 1; attempt <= maxRetries; attempt++ {
		res, err = rc.redis.MGet(ctx, keys...).Result()
		if err == nil {
			break
		}

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			rc.log.Error("context canceled during redis mget", "err", err)
			return nil, nil, err
		}

		rc.log.Warn("redis mget failed, retrying...", "attempt", attempt+1, "err", err)

		backoff := time.Millisecond * time.Duration(50*(attempt+1))
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(backoff):
		}
	}

	if err != nil {
		rc.log.Error("error mget exec", "err", err)
		return nil, nil, err
	}

	hits := make(map[uint64]uuid.UUID, len(fps))
	missed := make([]uint64, 0, len(fps))
	for i, val := range res {
		id, ok := val.(string)
		if !ok {
			missed = append(missed, fps[i])
			continue
		}

		uu, err := uuid.Parse(id)
		if uu == uuid.Nil {

		}

		if err != nil {
			rc.log.Error("unknown fingerprint id type of fingerprint", "fingerprint", fps[i], "value", id)
			missed = append(missed, fps[i])
			continue
		}

		hits[fps[i]] = uu
	}

	return hits, missed, nil
}

func (rc *RedisCache) SaveTemp(ctx context.Context, fprints []uint64) ([]uint64, []uint64, error) {
	if len(fprints) == 0 {
		return nil, nil, fmt.Errorf("got empty fingerprints slice")
	}

	buffer := rc.bufferPool.Get().(buffers.NumBuffer)
	defer rc.bufferPool.Put(buffer)
	uunil := uuid.Nil.String()
	pipe := rc.redis.Pipeline()

	cmds := make([]*redis.BoolCmd, len(fprints))
	for i, v := range fprints {
		cmds[i] = pipe.SetNX(ctx, buffer.Convert(v), uunil, 5*time.Second)
	}

	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			rc.log.Error("context canceled during redis pipeline exec", "err", err)
			return nil, nil, err
		}
		rc.log.Error("unknown redis error during pipeline exec", "err", err)
		return nil, nil, err
	}

	setted, skipped := make([]uint64, 0, len(fprints)), make([]uint64, 0, len(fprints))
	for i, v := range cmds {
		success, err := v.Result()
		if err != nil {
			rc.log.Error("unable to set temp uuid on fingerprint with error", "err", err, "fingerprint", fprints[i])
			skipped = append(skipped, fprints[i])
			continue
		}

		if success == true {
			setted = append(setted, fprints[i])
		} else {
			skipped = append(skipped, fprints[i])
		}
	}

	return setted, skipped, nil
}

func (rc *RedisCache) SaveNew(ctx context.Context, values map[uint64]uuid.UUID) error {
	if len(values) == 0 {
		return nil
	}

	buffer := rc.bufferPool.Get().(buffers.NumBuffer)
	defer rc.bufferPool.Put(buffer)

	pipe := rc.redis.Pipeline()
	ttl := 24 * time.Hour

	for k, v := range values {
		key := buffer.Convert(k)
		val := v.String()

		pipe.Set(ctx, key, val, ttl)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			rc.log.Error("context canceled during redis pipeline save", "err", err)
			return err
		}
		rc.log.Error("unknown redis error during save", "err", err)
		return err
	}

	return nil
}
