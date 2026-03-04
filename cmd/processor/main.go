package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/pubsub"
	"github.com/censys/scan-takehome/pkg/metrics"
	"github.com/censys/scan-takehome/pkg/processor"
	"github.com/censys/scan-takehome/pkg/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	metricsAddr    := flag.String("metrics-addr", ":8080", "Address for the /metrics and /healthz HTTP server")
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

	// Isolated Prometheus registry — avoids polluting (or inheriting from) the
	// global default registry, which is best practice for server applications.
	reg := prometheus.NewRegistry()
	rec := metrics.NewRecorder(reg)

	// Serve /metrics (Prometheus scrape endpoint) and /healthz (liveness probe)
	// in a background goroutine alongside the main Pub/Sub receive loop.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	go func() {
		slog.Info("starting HTTP server", "addr", *metricsAddr)
		if err := http.ListenAndServe(*metricsAddr, mux); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server failed", "error", err)
			os.Exit(1)
		}
	}()

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

	proc := processor.NewWithRecorder(s, rec)

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
