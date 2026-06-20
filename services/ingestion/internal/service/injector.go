package service

import (
	"context"
	"pulseguard/services/pkg/contracts"
)

type InjectionService interface {
	validateProjectKey(context.Context, string) (int, error)
	ValidateErrorEvent(context.Context, *contracts.ErrorEvent, string) error
}
