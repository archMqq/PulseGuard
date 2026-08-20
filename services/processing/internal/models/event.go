package models

import (
	"encoding/json"
	"pulseguard/services/pkg/contracts"
	"strings"
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
	Payload    []byte
	TimeStamp  time.Time
	ReceivedAt time.Time
}

func ParseErrorEvent(ee contracts.ErrorEvent, issueId uuid.UUID) ErrorEvent {
	return ErrorEvent{
		IssueId: issueId,
		Level:   parseLevel(ee.Level),
		Payload: parsePayload(ee),
	}
}

func parseLevel(level string) uint8 {
	switch strings.ToLower(level) {
	case "trace":
		return 1
	case "debug":
		return 2
	case "info":
		return 3
	case "warn":
		return 4
	case "error":
		return 5
	case "fatal":
		return 6
	default:
		return 0
	}
}

func parsePayload(ee contracts.ErrorEvent) []byte {
	if len(ee.StackTrace) == 0 {
		return []byte("{\n\"unknown\"\n}")
	}

	js, err := json.Marshal(ee.StackTrace)
	if err != nil {
		return []byte("{\n\"unknown\"\n}")
	}

	return js
}
