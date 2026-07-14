package consumer

import (
	"context"
	"errors"
	"pulseguard/services/pkg/logger"
	"pulseguard/services/processing/internal/config"

	"github.com/segmentio/kafka-go"
)

var (
	KafkaMessageReadingErr = errors.New("error reading kafka message")
)

type KafkaConsumer struct {
	consumer    *kafka.Reader
	eventCh     chan<- kafka.Message
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

func (kc *KafkaConsumer) Start(ctx context.Context, eventCh chan<- kafka.Message, processedCh <-chan *[]kafka.Message) {
	kc.eventCh = eventCh
	kc.processedCh = processedCh

	go kc.consume(ctx)
	go kc.commit(ctx)
}

func (kc *KafkaConsumer) Close() error {
	close(kc.eventCh)
	return kc.consumer.Close()
}

func (kc KafkaConsumer) consume(ctx context.Context) {
	for {
		msg, err := kc.consumer.FetchMessage(ctx)

		if err != nil {
			if errors.Is(err, context.Canceled) {
				continue
			}
			kc.log.Warn(KafkaMessageReadingErr.Error(), msg.Topic, err)
			continue
		}
		kc.eventCh <- msg
	}
}

func (kc KafkaConsumer) commit(ctx context.Context) {
	done := <-kc.processedCh

	if err := kc.consumer.CommitMessages(ctx, (*done)...); err != nil {
		kc.log.Warn("error msg read commit", err)
	}
}
