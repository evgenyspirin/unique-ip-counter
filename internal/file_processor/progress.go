package file_processor

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

const interval = 5 * time.Second

type Progress struct {
	logger *zap.Logger
	done   atomic.Int64
	last   atomic.Int64
}

func NewProgress(
	logger *zap.Logger,
) *Progress {
	return &Progress{
		logger: logger,
	}
}

func (p *Progress) Add(n int64) { _ = p.done.Add(n) }

func (p *Progress) Run(totalSize int64) (stop func()) {
	t := time.NewTicker(interval)
	done := make(chan struct{})

	human := func(b uint64) string {
		const (
			KB = 1 << 10
			MB = 1 << 20
			GB = 1 << 30
		)
		switch {
		case b >= GB:
			return fmt.Sprintf("%.2fGB", float64(b)/GB)
		case b >= MB:
			return fmt.Sprintf("%.2fMB", float64(b)/MB)
		case b >= KB:
			return fmt.Sprintf("%.2fKB", float64(b)/KB)
		default:
			return fmt.Sprintf("%dB", b)
		}
	}

	go func() {
		defer t.Stop()
		for {
			select {
			case <-t.C:
				total := totalSize
				if total <= 0 {
					continue
				}
				d := p.done.Load()
				if d > total {
					d = total
				}
				pct := d * 100 / total
				if pct > p.last.Load() {
					p.last.Store(pct)

					var ms runtime.MemStats
					runtime.ReadMemStats(&ms)

					p.logger.Sugar().Infof(
						"progress: %d%% | alloc=%s heap_inuse=%s gc_cycles=%d | goroutines=%d ",
						pct,
						human(ms.Alloc),
						human(ms.HeapInuse),
						ms.NumGC,
						runtime.NumGoroutine(),
					)
				}
				if d >= total || pct >= 100 {
					return
				}
			case <-done:
				return
			}
		}
	}()

	return func() { close(done) }
}
