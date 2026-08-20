package cache

import (
	"context"

	"github.com/google/uuid"
)

type Cache interface {
	CheckIssues(context.Context, []uint64) (map[uint64]uuid.UUID, []uint64, error)
	SaveTempValues(context.Context, ...uint64) ([]uint64, []uint64, error)
	SaveNewIssues(context.Context, map[uint64]uuid.UUID) error
	IncrementIssues(context.Context, map[uuid.UUID]int) error
	LockAllert(context.Context, ...uuid.UUID) ([]uuid.UUID, error)
}
