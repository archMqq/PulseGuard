package queue

import "fmt"

type KafkaWritingErr struct {
	msg error
}

func NewKafkaWritingErr(topic string, err error) KafkaWritingErr {
	return KafkaWritingErr{
		msg: fmt.Errorf("error kafka writing to %s: %w", topic, err),
	}
}

func (kwe KafkaWritingErr) Error() string {
	return kwe.msg.Error()
}
