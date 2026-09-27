package conduit

import (
	"errors"
	"fmt"
	"time"
)

// Config configures Kaflume's queue-neutral processing behavior. Transport
// connection settings belong to the selected Adapter.
type Config struct {
	BatchSize int
	BatchWait time.Duration
}

func (c Config) validate() error {
	if c.BatchSize < 0 { return fmt.Errorf("kaflume: batch size must not be negative") }
	if c.BatchWait < 0 { return fmt.Errorf("kaflume: batch wait must not be negative") }
	if c.BatchSize > 0 && c.BatchWait == 0 { return errors.New("kaflume: batch wait is required when batch size is set") }
	return nil
}
