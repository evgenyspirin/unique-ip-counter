package main

import (
	"context"
	"log"
	"os"
	"unique-ip-counter/internal"
)

func main() {
	ctx := context.Background()

	app, err := internal.NewApp()
	if err != nil {
		log.Fatalf("init app failed: %v", err)
	}
	defer app.Close()

	if err = app.Run(ctx); err != nil {
		app.Logger().Sugar().Errorf("uIPCounter stopped with error: %v", err)
		os.Exit(1)
	}
}
