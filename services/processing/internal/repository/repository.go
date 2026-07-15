package repository

import (
	"context"
	"pulseguard/services/processing/internal/repository/models"
)

type Repository interface {
	SaveIssue(context.Context, models.ErrorIssue)
	
}
