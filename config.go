package conduit

import (
	"errors"
	"fmt"
	"time"
)

// Config configures Conduit's queue-neutral processing behavior. Transport
// connection settings belong to the selected Adapter.
type Config struct {
	BatchSize  int
	BatchWait  time.Duration
	Retry      RetryPolicy
	DeadLetter DeadLetterHandler
	Metrics    Metrics
	Logger     Logger
}

func (c Config) validate() error {
	if c.BatchSize < 0 {
		return fmt.Errorf("conduit: batch size must not be negative")
	}
	if c.BatchWait < 0 {
		return fmt.Errorf("conduit: batch wait must not be negative")
	}
	if c.BatchSize > 0 && c.BatchWait == 0 {
		return errors.New("conduit: batch wait is required when batch size is set")
	}
	if err := c.Retry.validate(); err != nil {
		return err
	}
	return nil
}
