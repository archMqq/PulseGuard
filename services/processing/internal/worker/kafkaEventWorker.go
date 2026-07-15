package worker

import (
	"context"
	"encoding/json"
	"pulseguard/services/pkg/contracts"
	"pulseguard/services/pkg/logger"
	"pulseguard/services/processing/internal/cache"
	"pulseguard/services/processing/internal/consumer"
	"pulseguard/services/processing/internal/fingerprint"
	"pulseguard/services/processing/internal/repository"

	"github.com/segmentio/kafka-go"
)

type KafkaEventWorker struct {
	consumer *consumer.KafkaConsumer
	cache    cache.Cache
	repo     repository.Repository
	fper     fingerprint.Fingerprinter
	log      logger.Logger
}

func NewKafkaEventWorker(ctx context.Context, consumer *consumer.KafkaConsumer,
	cache cache.Cache, repo repository.Repository, fper fingerprint.Fingerprinter,
	log logger.Logger) {

}

func (ew KafkaEventWorker) Start(ctx context.Context) {
	eventCh := make(chan *[]kafka.Message)
	processedCh := make(chan *[]kafka.Message)
	ew.consumer.Start(ctx, eventCh, processedCh)
}

func (ew KafkaEventWorker) Close() error {
	// TODO: implement
	return nil
}

func (ew KafkaEventWorker) processMsg(ctx context.Context, eventCh chan *[]kafka.Message,
	processCh chan *[]kafka.Message) {
	select {
	case <-ctx.Done():
		return
	default:
		batch := <-eventCh
		fps := make(map[uint64]int32, len(*batch))

		for i, v := range *batch {
			var event contracts.ErrorEvent
			err := json.Unmarshal(v.Value, &event)
			if err != nil {
				ew.log.Warn("error marshalling error event", err)
				continue
			}

			fp := ew.fper.GenerateFingerprint(event)
			fps[fp] = int32(i)

			uu, err := ew.cache.CheckEvent(ctx, fp)
			if err != nil {
				//TODO: push temp val to redis, save event to postgres
			}
			uu.ID() // TODO: increment redis issue count
		}
		processCh <- batch
	}

}
