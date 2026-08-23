package syslogrx

import (
	"context"
	"time"
)

// WriteContext delays the write by d but returns as soon as ctx is cancelled
// or hits its deadline, instead of holding the caller hostage inside the
// internal delay. The prior form called time.Sleep(d) unconditionally, so a
// done context still blocked for the full d; cancellation was imperceptible
// and the overall write timeout was dragged out, producing collection-latency
// spikes. Once ctx is done the write path must wind down promptly rather than
// dead-waiting on the delay to wake it up.
func WriteContext(ctx context.Context, s Sink, m *Message, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d <= 0 {
		return s.Write(m)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
		return ctx.Err()
	}
	// Re-check after the delay elapses: if ctx went done in the same window
	// the timer fired, wind down instead of handing off to the sink.
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.Write(m)
}
