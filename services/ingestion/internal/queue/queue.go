package queue

import "context"

type QueueSaver interface {
	Save(context.Context, []byte) error
}
