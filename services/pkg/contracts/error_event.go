package contracts

type ErrorEvent struct {
	ProjectKey int         `json:"project_id" validate:"required min=1"`
	Type       string      `json:"type" validate:"required"`
	Level      string      `json:"level" validate:"required oneof=fatal error warn info debug trace"`
	Message    string      `json:"message" validate:"required max=255"`
	Timestamp  int64       `json:"timestamp" validate:"required"`
	StackTrace []TraceElem `json:"stack_trace" validate:"required min=1"`
}

type TraceElem struct {
	File   string `json:"file" validate:"required"`
	Method string `json:"method" validate:"required"`
	Line   int    `json:"line" validate:"required min=1"`
}
