package conduit

import "fmt"

// ErrorStage identifies the processing stage that failed.
type ErrorStage string

const (
	StageTransport   ErrorStage = "transport"
	StageDeserialize ErrorStage = "deserialize"
	StageProcess     ErrorStage = "process"
	StageDeadLetter  ErrorStage = "dead_letter"
	StageAcknowledge ErrorStage = "acknowledge"
)

// Error adds a stable stage and message location to an underlying error.
// Callers can inspect it with errors.As and the cause with errors.Is.
type Error struct {
	Stage     ErrorStage
	Topic     string
	Partition int32
	Offset    int64
	Err       error
}

func (e *Error) Error() string {
	if e.Topic == "" {
		return fmt.Sprintf("conduit: %s: %v", e.Stage, e.Err)
	}
	return fmt.Sprintf("conduit: %s %s/%d/%d: %v", e.Stage, e.Topic, e.Partition, e.Offset, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }
