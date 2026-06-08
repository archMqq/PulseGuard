package service

import (
	"pulseguard/services/identity/internal/repository"
	"time"

	"github.com/maypok86/otter/v2"
)

type IdentityService struct {
	cache *otter.Cache[string,string]
	identityRepo *repository.IdentityRepository
}

func New(repo *repository.IdentityRepository) *IdentityService {
	cache := otter.Must(&otter.Options[string, string]{
		MaximumSize: 10_000,
		ExpiryCalculator: otter.ExpiryAccessing[string, string](time.Second),
		RefreshCalculator: otter.RefreshWriting[string, string](500 * time.Millisecond),
	})
	return &IdentityService{
		cache: cache,
		identityRepo: repo,
	}
}
