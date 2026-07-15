package models

import (
	"time"
)

type ErrorEvent struct {
	Id         int64
	IssueId    uuid
	Level      Level // TODO: enum Level with int represent
	Payload    string
	TimeStamp  time.Time
	ReceivedAt time.Time
}
