package models

import (
	"time"
	"github.com/google/uuid"
)

const (
	trace = iota + 1
	debug
	info
	warn
	error
	fatal
)

type ErrorEvent struct {
	Id         int64
	IssueId    uuid.UUID
	Level      uint8
	Payload    string
	TimeStamp  time.Time
	ReceivedAt time.Time
}
