package queue

import (
	"context"
	"fmt"
	"pulseguard/services/ingestion/internal/config"
	"pulseguard/services/pkg/logger"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaQueue struct {
	writer *kafka.Writer
}

func NewKafkaQueue(cfg config.KafkaConfig, log logger.Logger) *KafkaQueue {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Addr...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    cfg.BatchSize,
		BatchTimeout: time.Duration(cfg.BatchTimeout) * time.Millisecond,
	}

	writer.ErrorLogger = kafka.LoggerFunc(log.Error)

	return &KafkaQueue{
		writer: writer,
	}
}

func (kq KafkaQueue) Save(ctx context.Context, msg []byte) error {
	message := kafka.Message{
		Value: msg,
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

func (kq KafkaQueue) Close() error {
	return kq.writer.Close()
}
