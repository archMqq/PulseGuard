package queue

import (
	"context"
	"fmt"
	"pulseguard/services/ingestion/internal/config"

	"github.com/segmentio/kafka-go"
)

type KafkaQueue struct {
	writer *kafka.Writer
}

func NewKafkaQueue(cfg config.KafkaConfig) *KafkaQueue {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(cfg.Addr...),
		Topic:    cfg.Topic,
		Balancer: &kafka.LeastBytes{},
	}

	return &KafkaQueue{
		writer: writer,
	}
}

func (kq KafkaQueue) Save(ctx context.Context, msg string) error {
	message := kafka.Message{
		Value: []byte(msg),
	}

	switch err := kq.writer.WriteMessages(ctx, message).(type) {
	case nil:
		return nil
	case kafka.WriteErrors:
		return NewKafkaWritingErr(kq.writer.Topic, err)
	default:
		ctxE := ctx.Err()
		if ctxE != nil {
			return ctx.Err()
		}

		return fmt.Errorf("unknown error")
	}

}
