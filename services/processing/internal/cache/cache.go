package cache

import (
	"context"
)

type Cache interface {
	CheckEvent(context.Context, uint64)
	SaveTemp(context.Context)
	SaveNew(context.Context, uint64)
}
