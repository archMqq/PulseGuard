package cache

import (
	"context"

	"github.com/google/uuid"
)

type Cache interface {
	CheckEvent(context.Context, uint64) (uuid.UUID, error)
	SaveTemp(context.Context) error
	SaveNew(context.Context, uint64) error
}
