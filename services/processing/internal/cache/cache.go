package cache

import (
	"context"

	"github.com/google/uuid"
)

type Cache interface {
	CheckIssues(context.Context, []uint64) (map[uint64]uuid.UUID, []uint64, error)
	SaveTemp(context.Context, []uint64) ([]uint64, []uint64, error)
	SaveNew(context.Context, map[uint64]uuid.UUID) error
}
