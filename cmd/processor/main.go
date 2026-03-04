package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/pubsub"
	"github.com/censys/scan-takehome/pkg/processor"
	"github.com/censys/scan-takehome/pkg/store"
)

func main() {
	// JSON-structured logging so every field is machine-readable by log
	// aggregators (Datadog, Cloud Logging, etc.) without custom parsers.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	projectID      := flag.String("project", "test-project", "GCP project ID")
	subID          := flag.String("subscription", "scan-sub", "Pub/Sub subscription ID")
	dbPath         := flag.String("db", "/data/scans.db", "Path to SQLite database file")
	maxOutstanding := flag.Int("concurrency", 10, "Max outstanding messages per pull")
	flag.Parse()

	// Allow the project ID to be overridden via environment variable, matching
	// the docker-compose convention used by the scanner service.
	if v := os.Getenv("PUBSUB_PROJECT_ID"); v != "" {
		*projectID = v
	}

	// signal.NotifyContext cancels ctx on SIGINT or SIGTERM, which propagates
	// into sub.Receive — causing it to drain in-flight handlers before returning.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s, err := store.NewSQLiteStore(*dbPath)
	if err != nil {
		slog.Error("failed to init store", "error", err)
		os.Exit(1)
	}
	defer s.Close()

	// When PUBSUB_EMULATOR_HOST is set in the environment, the Pub/Sub client
	// automatically connects to the local emulator — no code change required.
	client, err := pubsub.NewClient(ctx, *projectID)
	if err != nil {
		slog.Error("failed to create pubsub client", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	sub := client.Subscription(*subID)
	sub.ReceiveSettings.MaxOutstandingMessages = *maxOutstanding

	proc := processor.New(s)

	slog.Info("processor started",
		"project", *projectID,
		"subscription", *subID,
		"db", *dbPath,
		"concurrency", *maxOutstanding,
	)

	// Receive blocks until ctx is cancelled or a non-retryable error occurs.
	// Each message is dispatched to proc.HandleMessage in its own goroutine;
	// Ack/Nack is the processor's responsibility.
	if err := sub.Receive(ctx, proc.HandleMessage); err != nil && ctx.Err() == nil {
		slog.Error("receive error", "error", err)
		os.Exit(1)
	}

	slog.Info("processor shut down cleanly")
}
