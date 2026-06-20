package service

import (
	"context"
	"errors"
	"pulseguard/services/ingestion/cache"
	"pulseguard/services/pkg/contracts"

	"github.com/go-playground/validator/v10"
)

var (
	ErrUnknownProject = errors.New("got unknown project id")
)

type ErrorInjectionService struct {
	validator *validator.Validate
	pc        cache.ProjectsCache
}

func NewErrInjectionService(pc cache.ProjectsCache) ErrorInjectionService {
	return ErrorInjectionService{
		validator: validator.New(),
		pc:        pc,
	}
}

func (er ErrorInjectionService) ValidateErrorEvent(ctx context.Context, event *contracts.ErrorEvent, key string) error {
	id, err := er.validateProjectKey(ctx, key)
	if err != nil {
		return ErrUnknownProject
	}

	event.ProjectId = id

	return er.validator.Struct(event)
}

func (er *ErrorInjectionService) validateProjectKey(ctx context.Context, key string) (int, error) {
	id, err := er.pc.CheckKey(ctx, key)
	if err != nil {
		// TODO logger
		return 0, err
	}

	return id, nil
}
