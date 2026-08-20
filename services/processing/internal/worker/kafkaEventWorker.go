package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"pulseguard/services/pkg/contracts"
	"pulseguard/services/pkg/logger"
	"pulseguard/services/processing/internal/cache"
	"pulseguard/services/processing/internal/consumer"
	"pulseguard/services/processing/internal/fingerprint"
	"pulseguard/services/processing/internal/models"
	"pulseguard/services/processing/internal/repository"
	"time"

	"github.com/google/uuid"

	"github.com/segmentio/kafka-go"
)

type KafkaEventWorker struct {
	consumer *consumer.KafkaConsumer
	cache    cache.Cache
	repo     repository.ErrorRepository
	fper     fingerprint.Fingerprinter
	log      logger.Logger
}

func NewKafkaEventWorker(ctx context.Context, consumer *consumer.KafkaConsumer,
	cache cache.Cache, repo repository.ErrorRepository, fper fingerprint.Fingerprinter,
	log logger.Logger) {

}

func (ew *KafkaEventWorker) Start(ctx context.Context) {
	eventCh := make(chan *[]kafka.Message)
	processedCh := make(chan *[]kafka.Message)
	ew.consumer.Start(ctx, eventCh, processedCh)

	go ew.processErrors(ctx, eventCh, processedCh)
}

func (ew *KafkaEventWorker) Close() error {
	if err := ew.consumer.Close(); err != nil {
		return err
	}
	// cache close
	if closer, ok := ew.cache.(io.Closer); ok {
		err := closer.Close()
		if err != nil {
			return err
		}
	}
	// repository close
	if closer, ok := ew.repo.(io.Closer); ok {
		err := closer.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

func (ew *KafkaEventWorker) processErrors(ctx context.Context, eventCh chan *[]kafka.Message,
	processedCh chan *[]kafka.Message) {
	type extractedEvent struct {
		fp    uint64
		issue models.ErrorIssue
		event models.ErrorEvent
	}

	for {
		select {
		case <-ctx.Done():
			return
		case batch, ok := <-eventCh:
			if !ok {
				return
			}

			if err := ew.processBatch(ctx, batch); err != nil {
				ew.log.Error("failed to process batch", "err", err, "batch_size", len(*batch))
				continue
			}

			processedCh <- batch
		}
	}
}

func (ew *KafkaEventWorker) processBatch(ctx context.Context, batch *[]kafka.Message) error {
	fps, events := ew.groupMessages(batch)

	if len(fps) == 0 {
		ew.log.Warn("got an unvalid batch of events")
		return nil
	}

	keys := ew.extractKeys(fps)
	hits, missed, err := ew.cache.CheckIssues(ctx, keys)
	if err != nil {
		return fmt.Errorf("failed to check event: %w", err)
		// TODO: обработка
	}

	var newIssues []uint64
	var resolvedHits map[uint64]uuid.UUID

	if len(missed) > 0 {
		newIssues, resolvedHits, err = ew.cycleCacheSaveTemp(ctx, missed)
		if err != nil {
			return fmt.Errorf("cache sync cycle failed: %w", err)
		}
	}

	if hits == nil {
		hits = make(map[uint64]uuid.UUID, len(fps))
	}
	for fp, id := range resolvedHits {
		hits[fp] = id
	}

	if len(newIssues) > 0 {
		issues := ew.transformFpToIssue(fps, newIssues, events)

		newIds, err := ew.repo.SaveNewIssues(ctx, issues)
		if err != nil {
			return fmt.Errorf("failed to save new issues: %w", err)
			// можно добавить retry, чтобы не потерять ошибки
		}

		if len(newIds) > 0 {
			go ew.asyncSaveToCache(newIds)
		}

		for fp, id := range newIds {
			hits[fp] = id
		}
	}

	dbEvents := ew.transformFpToEvent(fps, hits, events)
	err = ew.repo.SaveErrorEvents(ctx, dbEvents)
	if err != nil {
		return fmt.Errorf("failed to save error events: %w", err)
	}

	return nil
}

func (ew *KafkaEventWorker) groupMessages(batch *[]kafka.Message) (map[uint64][]int32, []contracts.ErrorEvent) {
	fps := make(map[uint64][]int32)
	events := make([]contracts.ErrorEvent, len(*batch))
	for i, v := range *batch {
		err := json.Unmarshal(v.Value, &events[i])

		if err != nil {
			ew.log.Warn("error marshalling error event", "err", err)
			continue
		}

		fp := ew.fper.GenerateFingerprint(events[i])
		fps[fp] = append(fps[fp], int32(i))
	}

	return fps, events
}

func (ew *KafkaEventWorker) extractKeys(fps map[uint64][]int32) []uint64 {
	keys := make([]uint64, 0, len(fps))
	for k := range fps {
		keys = append(keys, k)
	}
	return keys
}

func (ew *KafkaEventWorker) transformFpToIssue(mp map[uint64][]int32, fps []uint64, ees []contracts.ErrorEvent) []models.ErrorIssue {
	issues := make([]models.ErrorIssue, len(fps))
	for i, v := range fps {
		idx := mp[v][0]
		issues[i] = models.ParseErrorIssue(ees[idx], v)
	}
	return issues
}

func (ew *KafkaEventWorker) transformFpToEvent(mp map[uint64][]int32, fps map[uint64]uuid.UUID, ees []contracts.ErrorEvent) []models.ErrorEvent {
	events := make([]models.ErrorEvent, 0, len(ees))
	for fp, uu := range fps {
		eventIds := mp[fp]
		for _, v := range eventIds {
			event := ees[v]
			events = append(events, models.ParseErrorEvent(event, uu))
		}
	}
	return events
}

func (ew *KafkaEventWorker) cycleCacheSaveTemp(ctx context.Context, fps []uint64) ([]uint64, map[uint64]uuid.UUID, error) {
	setted, skipped, err := ew.cache.SaveTempValues(ctx, fps...)
	if err != nil {
		return nil, nil, fmt.Errorf("error saving temp values in cache: %w", err)
	}

	resolvedHits := make(map[uint64]uuid.UUID)

	if len(skipped) > 0 {
		const retries = 3
		const delay = 15

		for attempt := 1; attempt <= retries; attempt++ {
			backoff := time.Millisecond * time.Duration(delay*(attempt+1))
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-time.After(backoff):
			}

			hits, misses, err := ew.cache.CheckIssues(ctx, skipped)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to save temp issue keys: %w", err)
			}

			for fp, id := range hits {
				resolvedHits[fp] = id
			}

			skipped = misses

			if len(skipped) == 0 {
				break
			}

			ew.log.Info("check issue got misses, retrying...", "attempt", attempt+1)
		}

		if len(skipped) > 0 {
			return nil, nil, fmt.Errorf("failed to resolve %d issues after %d retries", len(skipped), retries)
		}
	}
	return setted, resolvedHits, nil
}

// asyncSaveToCache выполняет асинхронное сохранение новых UUID инцидентов в Redis.
// Метод сам управляет своим жизненным циклом (контекстом и ретраями),
// чтобы не блокировать консьюмер Kafka.
func (ew *KafkaEventWorker) asyncSaveToCache(ids map[uint64]uuid.UUID) {
	bgCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const maxRetries = 3
	const delay = 100

	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := ew.cache.SaveNewIssues(bgCtx, ids)
		if err == nil {
			return
		}

		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			ew.log.Error("async cache save context deadline exceeded", "err", err)
			break
		}

		ew.log.Warn("failed to async save new issues to cache, retrying...",
			"attempt", attempt,
			"err", err,
		)

		backoff := time.Millisecond * time.Duration(delay*attempt)
		select {
		case <-bgCtx.Done():
			ew.log.Error("context canceled during backoff sleep")
			return
		case <-time.After(backoff):
		}
	}

	ew.log.Error("CRITICAL: failed to save new issues to cache after all retries. DB will face extra load on next events.")
}
