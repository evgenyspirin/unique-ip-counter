package internal

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"unique-ip-counter/internal/file_processor"
	"unique-ip-counter/internal/ipv4_bitset"
)

type App struct {
	logger *zap.Logger
	fp     *file_processor.FileProcessor
	done   chan struct{}
}

func NewApp() (*App, error) {
	// logger
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("cannot initialize zap logger: %v", err)
	}
	defer logger.Sync()

	// pars run args
	var (
		path string
		th   int
	)
	flag.StringVar(&path, "f", "", "path to file")
	flag.IntVar(&th, "th", runtime.NumCPU(), "count of goroutines + shards")
	flag.Parse()
	if path == "" {
		log.Fatal("please provide path to file")
	}

	// file processor
	f, err := os.Open(path)
	if err != nil {
		log.Fatalf("cannot open the file: %v", err)
	}
	fp := file_processor.New(logger, f, ipv4_bitset.New(), th)

	return &App{
		logger: logger,
		fp:     fp,
		done:   make(chan struct{}),
	}, nil
}

func (a *App) Close() {
	if a.fp.GetFile() != nil {
		_ = a.fp.GetFile().Close()
	}
	if a.logger != nil {
		_ = a.logger.Sync()
	}
}

func (a *App) Run(ctx context.Context) error {
	a.logger.Info("running uIPCounter...")

	// context with os signals cancel chan
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGUSR1)
	defer stop()

	// "errgroup" instead of "WaitGroup" because:
	// - allows return an error from goroutine
	// - group errors from multiple gorutines into one
	// - wg.Add(1), wg.Done() - automatically under the hood, so never catch deadlock if you forget something ;-)
	// - allows orchestration of parallel processes through the context.Context(gracefull shut down)
	start := time.Now()
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		fi, err := a.fp.GetFile().Stat()
		if err != nil {
			return err
		}
		if err = a.fp.ProcessFile(ctx, fi); err != nil {
			return fmt.Errorf("FileProcessor error: %w", err)
		}
		a.done <- struct{}{}

		return nil
	})

	// waiting when processing file finished or sigurg signal
	select {
	case <-a.done:
		fmt.Printf("unique ip's: %v, total time: %v sec\n", a.fp.UniqueCount(), time.Since(start).Seconds())
	case <-ctx.Done():
	}

	if err := g.Wait(); err != nil {
		a.logger.Error("uIPCounter returning an error", zap.Error(err))
		return err
	}

	a.logger.Info("uIPCounter exited properly")

	return nil
}

func (a *App) Logger() *zap.Logger { return a.logger }
