package async

import (
	"context"
	"log"
	"reflect"
	"runtime"
	"time"
)

// RunEvery runs the provided command periodically.
// It runs in a goroutine, and can be cancelled by finishing the supplied context.
func RunEvery(ctx context.Context, period time.Duration, f func()) {
	funcName := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
	ticker := time.NewTicker(period)
	go func() {
		for {
			select {
			case <-ticker.C:
				log.Printf("function %s running", funcName)
				f()
			case <-ctx.Done():
				log.Printf("function %s exiting as context closed", funcName)
				ticker.Stop()
				return
			}
		}
	}()
}
