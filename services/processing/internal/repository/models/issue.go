package models

import (
	"pulseguard/services/pkg/contracts"
	"time"
)

type ErrorIssue struct {
	Id          uuid
	ProjectId   int
	Fingerprint uint64
	Title       string
	Status      string
	UpdatedAt   time.Time
	CreatedAt   time.Time
}

func ParseErrorIssue(ee contracts.ErrorEvent, fp uint64) *ErrorIssue {
	return &ErrorIssue{
		ProjectId:   ee.ProjectId,
		Fingerprint: fp,
		Title:       ee.Type,
		Status:      "created",
		UpdatedAt:   time.Now(),
		CreatedAt:   time.UnixMicro(ee.Timestamp),
	}
}
