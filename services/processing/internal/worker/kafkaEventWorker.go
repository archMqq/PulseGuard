package worker

import (
	"context"
	"pulseguard/services/processing/internal/cache"
	"pulseguard/services/processing/internal/consumer"
	"pulseguard/services/processing/internal/repository"

	"github.com/segmentio/kafka-go"
)

type KafkaEventWorker struct {
	consumer *consumer.KafkaConsumer
	cache    cache.Cache
	repo     repository.Repository
}

func NewKafkaEventWorker(ctx context.Context)

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
	batch := <- eventCh
	fps := make([]uint64, 0, len(*batch))

	for i, v := range *batch {
		fp := fingerprint.New(ctx, v.Value)
		fps[i] = fp
	}

	
}
