package consumer

import (
	"context"
	"errors"
	"pulseguard/services/pkg/logger"
	"pulseguard/services/processing/internal/config"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	KafkaMessageReadingErr = errors.New("error reading kafka message")
)

type KafkaConsumer struct {
	consumer    *kafka.Reader
	eventCh     chan<- *[]kafka.Message
	processedCh <-chan *[]kafka.Message
	log         logger.Logger
}

func NewKafka(kc config.KafkaConfig, logger logger.Logger) *KafkaConsumer {
	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  kc.Brokers,
		GroupID:  kc.GroupId,
		Topic:    kc.Topic,
		MaxBytes: kc.MaxBytes,
	})

	return &KafkaConsumer{
		consumer: consumer,
		log:      logger,
	}
}

func (kc *KafkaConsumer) Start(ctx context.Context, eventCh chan<- *[]kafka.Message,
	processedCh <-chan *[]kafka.Message) {
	kc.eventCh = eventCh
	kc.processedCh = processedCh

	go kc.Consume(ctx)
}

func (kc *KafkaConsumer) Close() error {
	close(kc.eventCh)
	return kc.consumer.Close()
}

func (kc KafkaConsumer) Consume(ctx context.Context) {
	batchSize := 1000
	batchTimeout := 5 * time.Second

	batch := make([]kafka.Message, 0, batchSize)
	lastFlush := time.Now()
	for {
		if len(batch) > 0 && time.Since(lastFlush) >= batchTimeout {
			kc.processAndSave(ctx, &batch)
			lastFlush = time.Now()
		}
		msg, err := kc.consumer.FetchMessage(ctx)

		if err != nil {
			if errors.Is(err, context.Canceled) {
				continue
			}
			kc.log.Warn(KafkaMessageReadingErr.Error(), msg.Topic, err)
			continue
		}

		batch = append(batch, msg)

		if len(batch) >= batchSize {
			kc.processAndSave(ctx, &batch)
			lastFlush = time.Now()
		}
	}
}

func (kc KafkaConsumer) processAndSave(ctx context.Context, batch *[]kafka.Message) {
	if len(*batch) == 0 {
		return
	}

	kc.eventCh <- batch
	done := <-kc.processedCh

	if err := kc.consumer.CommitMessages(ctx, (*done)...); err != nil {
		kc.log.Warn("error msg read commit", err)
	}

	clear(*batch)
	*batch = (*batch)[:0]
}
