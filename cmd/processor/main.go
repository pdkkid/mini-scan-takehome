package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/censys/scan-takehome/pkg/metrics"
	"github.com/censys/scan-takehome/pkg/processor"
	"github.com/censys/scan-takehome/pkg/publish"
	"github.com/censys/scan-takehome/pkg/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// healthStatus is the JSON body returned by the /healthz endpoint.
type healthStatus struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

func main() {
	// JSON-structured logging so every field is machine-readable by log
	// aggregators (Datadog, Cloud Logging, etc.) without custom parsers.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	projectID := flag.String("project", "test-project", "GCP project ID")
	subID := flag.String("subscription", "scan-sub", "Pub/Sub subscription ID")
	dlqTopicID := flag.String("dlq-topic", "scan-dlq", "Pub/Sub topic ID for the dead letter queue")
	storeType := flag.String("store", "sqlite", "Store backend: sqlite or postgres")
	dbPath := flag.String("db", "/data/scans.db", "Path to SQLite database file")
	postgresURL := flag.String("postgres-url", "", "PostgreSQL connection string (required when -store=postgres)")
	maxOutstanding := flag.Int("concurrency", 10, "Max outstanding messages per pull")
	metricsAddr := flag.String("metrics-addr", ":8080", "Address for the /metrics and /healthz HTTP server")
	flag.Parse()

	// Allow the project ID and Postgres URL to be overridden via environment
	// variables, matching the docker-compose convention used by the scanner.
	if v := os.Getenv("PUBSUB_PROJECT_ID"); v != "" {
		*projectID = v
	}
	if v := os.Getenv("POSTGRES_URL"); v != "" {
		*postgresURL = v
	}

	// signal.NotifyContext cancels ctx on SIGINT or SIGTERM, which propagates
	// into sub.Receive — causing it to drain in-flight handlers before returning.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Isolated Prometheus registry — avoids polluting (or inheriting from) the
	// global default registry, which is best practice for server applications.
	reg := prometheus.NewRegistry()
	rec := metrics.NewRecorder(reg)

	var s store.Store
	switch *storeType {
	case "sqlite":
		ss, err := store.NewSQLiteStore(*dbPath)
		if err != nil {
			slog.Error("failed to init sqlite store", "error", err)
			os.Exit(1)
		}
		s = ss
	case "postgres":
		if *postgresURL == "" {
			slog.Error("postgres-url is required when -store=postgres")
			os.Exit(1)
		}
		ps, err := store.NewPostgresStore(ctx, *postgresURL)
		if err != nil {
			slog.Error("failed to init postgres store", "error", err)
			os.Exit(1)
		}
		s = ps
	default:
		slog.Error("unknown store type", "store", *storeType)
		os.Exit(1)
	}
	defer func() {
		if err := s.Close(); err != nil {
			slog.Error("failed to close store", "error", err)
		}
	}()

	// When PUBSUB_EMULATOR_HOST is set in the environment, the Pub/Sub client
	// automatically connects to the local emulator — no code change required.
	client, err := pubsub.NewClient(ctx, *projectID)
	if err != nil {
		slog.Error("failed to create pubsub client", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := client.Close(); err != nil {
			slog.Error("failed to close pubsub client", "error", err)
		}
	}()

	sub := client.Subscription(*subID)
	sub.ReceiveSettings.MaxOutstandingMessages = *maxOutstanding

	// Serve /metrics (Prometheus scrape endpoint) and /healthz (readiness probe)
	// in a background goroutine alongside the main Pub/Sub receive loop.
	// The HTTP server is started after s and sub are initialised so the /healthz
	// handler can safely close over them without a race.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		// Cap each check at 3 s so a hung dependency can't block the probe forever.
		hctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		checks := make(map[string]string)
		healthy := true

		// Store health — db.PingContext opens/reuses a connection and round-trips
		// a lightweight query, confirming the file is accessible and not corrupt.
		if err := s.Ping(hctx); err != nil {
			checks["store"] = err.Error()
			healthy = false
		} else {
			checks["store"] = "ok"
		}

		// Pub/Sub health — Exists() performs a live gRPC call to the server and
		// confirms both reachability and that the subscription actually exists.
		if exists, err := sub.Exists(hctx); err != nil {
			checks["pubsub"] = err.Error()
			healthy = false
		} else if !exists {
			checks["pubsub"] = "subscription not found"
			healthy = false
		} else {
			checks["pubsub"] = "ok"
		}

		status := "ok"
		code := http.StatusOK
		if !healthy {
			status = "degraded"
			code = http.StatusServiceUnavailable
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(healthStatus{Status: status, Checks: checks}) //nolint:errcheck
	})
	go func() {
		slog.Info("starting HTTP server", "addr", *metricsAddr)
		if err := http.ListenAndServe(*metricsAddr, mux); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server failed", "error", err)
			os.Exit(1)
		}
	}()

	proc := processor.NewWithRecorder(s, rec)

	// Dead letter queue — permanent-error messages are forwarded here instead of
	// being silently dropped. The DLQ topic is always configured; if the topic
	// doesn't exist yet, publish will fail and the message is Nack'ed with a log.
	dlqTopic := client.Topic(*dlqTopicID)
	proc.SetDLQ(publish.NewTopicPublisher(dlqTopic))

	slog.Info("processor started",
		"project", *projectID,
		"subscription", *subID,
		"store", *storeType,
		"dlq_topic", dlqTopicID,
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
