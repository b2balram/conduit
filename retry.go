package conduit

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

// Backoff calculates the delay before the next attempt. attempt starts at 1
// after the first failed processing attempt.
type Backoff interface {
	Delay(attempt int) time.Duration
}

// BackoffFunc adapts a function into a Backoff implementation.
type BackoffFunc func(attempt int) time.Duration

func (f BackoffFunc) Delay(attempt int) time.Duration { return f(attempt) }

// FixedBackoff waits the same duration between attempts.
type FixedBackoff time.Duration

func (b FixedBackoff) Delay(int) time.Duration { return time.Duration(b) }

// ExponentialBackoff grows from Initial up to Max. Jitter is a fraction in the
// range [0, 1] used to randomize the delay and avoid synchronized retries.
type ExponentialBackoff struct {
	Initial time.Duration
	Max     time.Duration
	Jitter  float64
}

func (b ExponentialBackoff) Delay(attempt int) time.Duration {
	if attempt < 1 || b.Initial <= 0 {
		return 0
	}
	factor := math.Pow(2, float64(attempt-1))
	delayValue := float64(b.Initial) * factor
	if b.Max > 0 && delayValue > float64(b.Max) {
		delayValue = float64(b.Max)
	} else if delayValue > float64(math.MaxInt64) {
		delayValue = float64(math.MaxInt64)
	}
	delay := time.Duration(delayValue)
	if b.Jitter > 0 {
		spread := float64(delay) * b.Jitter
		delay = time.Duration(float64(delay) - spread + rand.Float64()*(2*spread))
	}
	return max(delay, 0)
}

// RetryPolicy controls in-process processor retries. MaxAttempts includes the
// initial attempt. Zero disables retries and behaves as one attempt.
type RetryPolicy struct {
	MaxAttempts int
	Backoff     Backoff
	Retryable   func(error) bool
}

func (p RetryPolicy) validate() error {
	if p.MaxAttempts < 0 {
		return fmt.Errorf("conduit: retry max attempts must not be negative")
	}
	if fixed, ok := p.Backoff.(FixedBackoff); ok && fixed < 0 {
		return fmt.Errorf("conduit: retry backoff duration must not be negative")
	}
	if exponential, ok := p.Backoff.(ExponentialBackoff); ok {
		if exponential.Initial < 0 || exponential.Max < 0 {
			return fmt.Errorf("conduit: retry backoff durations must not be negative")
		}
		if exponential.Jitter < 0 || exponential.Jitter > 1 {
			return fmt.Errorf("conduit: retry jitter must be between 0 and 1")
		}
	}
	return nil
}

func (p RetryPolicy) attempts() int {
	if p.MaxAttempts <= 1 {
		return 1
	}
	return p.MaxAttempts
}

func (p RetryPolicy) shouldRetry(err error) bool {
	return p.Retryable == nil || p.Retryable(err)
}

func (p RetryPolicy) wait(ctx context.Context, attempt int) error {
	if p.Backoff == nil {
		return nil
	}
	delay := p.Backoff.Delay(attempt)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
