package repository

import (
	"context"
	"pulseguard/services/processing/internal/models"

	"github.com/google/uuid"
)

type ErrorRepository interface {
	SaveNewIssues(context.Context, []models.ErrorIssue) (map[uint64]uuid.UUID, error)
	SaveErrorEvents(context.Context, []models.ErrorEvent) error
}
