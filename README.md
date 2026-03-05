# Mini-Scan

Hello!

As you've heard by now, Censys scans the internet at an incredible scale. Processing the results necessitates scaling horizontally across thousands of machines. One key aspect of our architecture is the use of distributed queues to pass data between machines.

---

The `docker-compose.yml` file sets up a toy example of a scanner. It spins up a Google Pub/Sub emulator, creates a topic and subscription, and publishes scan results to the topic. It can be run via `docker compose up`.

Your job is to build the data processing side. It should:

1. Pull scan results from the subscription `scan-sub`.
2. Maintain an up-to-date record of each unique `(ip, port, service)`. This should contain when the service was last scanned and a string containing the service's response.

> **_NOTE_**
> The scanner can publish data in two formats, shown below. In both of the following examples, the service response should be stored as: `"hello world"`.
>
> ```javascript
> {
>   // ...
>   "data_version": 1,
>   "data": {
>     "response_bytes_utf8": "aGVsbG8gd29ybGQ="
>   }
> }
>
> {
>   // ...
>   "data_version": 2,
>   "data": {
>     "response_str": "hello world"
>   }
> }
> ```

Your processing application should be able to be scaled horizontally, but this isn't something you need to actually do. The processing application should use `at-least-once` semantics where ever applicable.

You may write this in any languages you choose, but Go would be preferred.

You may use any data store of your choosing, with `sqlite` being one example. Like our own code, we expect the code structure to make it easy to switch data stores.

Please note that Google Pub/Sub is best effort ordering and we want to keep the latest scan. While the example scanner does not publish scans at a rate where this would be an issue, we expect the application to be able to handle extreme out of orderness. Consider what would happen if the application received a scan that is 24 hours old.

cmd/scanner/main.go should not be modified

---

Please upload the code to a publicly accessible GitHub, GitLab or other public code repository account. This README file should be updated, briefly documenting your solution. Like our own code, we expect testing instructions: whether it’s an automated test framework, or simple manual steps.

To help set expectations, we believe you should aim to take no more than 4 hours on this task.

We understand that you have other responsibilities, so if you think you’ll need more than 5 business days, just let us know when you expect to send a reply.

Please don’t hesitate to ask any follow-up questions for clarification.

---

## Solution

### Overview

The processor is implemented as a new `cmd/processor` binary. Created the following packages:

- **`pkg/store`** — a `Store` interface with three implementations: `SQLiteStore` (embedded, single-host), `PostgresStore` (networked, multi-replica), and `MemoryStore` (in-process, used in tests). Adding a new backend requires only implementing the interface.
- **`pkg/processor`** — message parsing, V1/V2 decoding, error classification, and Ack/Nack dispatch.
- **`pkg/metrics`** - prometheus instrumentation for the scan processor to provide metrics.
- **`pkg/publish`** - DQL publishing

### Design Decisions

**Store interface for swappability**
`pkg/store/store.go` defines a `Store` interface with `Upsert` and `Get` methods. The interface is proven by three concrete implementations (`SQLiteStore`, `PostgresStore`, and `MemoryStore`) that are verified against an identical behavioral test suite — if all pass, they are safely interchangeable.
**Atomic out-of-order protection**

The upsert uses a single atomic SQL statement:

```sql
INSERT INTO scan_records (ip, port, service, last_scanned, response)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(ip, port, service) DO UPDATE SET
    last_scanned = excluded.last_scanned,
    response     = excluded.response
WHERE excluded.last_scanned > scan_records.last_scanned;
```

The `WHERE` clause on `DO UPDATE` means a 24-hour-old message is silently ignored at the database level — no read-before-write, no application-level locking, no race between concurrent processors. `MemoryStore` applies the same logic in Go under a `sync.RWMutex`.
**Permanent vs. transient error handling**

Not all errors should be retried. The processor classifies failures into two categories:

- **Permanent** (malformed JSON, unknown `data_version`): the message can never be processed successfully — forward to the dead letter queue and `ACK` if successful, preventing an infinite redelivery loop while preserving the message for debugging.
- **Transient** (store unavailable, network error): failure is situational — `Nack` so Pub/Sub redelivers once the issue resolves.

`ErrPermanent` is an exported sentinel, making the classification testable with `errors.Is`.

**Dead letter queue**
Messages that produce permanent errors are forwarded to a separate Pub/Sub topic (`scan-dlq`) instead of being silently dropped. The original message bytes are preserved as-is in the DLQ message body; error context is added as Pub/Sub message attributes (`original_msg_id`, `original_publish_time`, `error`). This allows operators to inspect failed messages, diagnose the root cause, and potentially reprocess them after a fix.

The `Publisher` interface abstracts the DLQ topic, following the same swappable-backend pattern as `store.Store`. A new metric (`scan_dlq_published_total`) tracks successful DLQ publishes; comparing it against `scan_messages_processed_total{status="permanent"}` reveals DLQ publish failures.

**At-least-once semantics**
`msg.Nack()` is called on transient failures so Pub/Sub redelivers. `msg.Ack()` is called on success and on permanent failures which successfully publish to the DQL. Combined with the idempotent upsert, redeliveries are always safe.
**V1 / V2 parsing**

When `encoding/json` unmarshals into `scanning.Scan`, the `Data interface{}` field becomes `map[string]interface{}`. The processor re-marshals that map back to JSON bytes and unmarshals into the correct typed struct (`V1Data` or `V2Data`) based on `DataVersion`. For V1, Go’s `encoding/json` automatically base64-decodes `response_bytes_utf8` into `[]byte`.

**Horizontal scaling**
Multiple processor replicas can consume from the same Pub/Sub subscription — Pub/Sub load-balances automatically. The conditional upsert ensures correctness when two replicas race to write the same `(ip, port, service)`. The `PostgresStore` backend is the recommended choice for multi-replica deployments — its MVCC concurrency model allows parallel writers without blocking, and connection pooling via `pgxpool` handles high concurrency natively. `SQLiteStore` is still available for single-replica or local development use cases.

**Structured logging**
The processor uses Go's stdlib `log/slog` package (introduced in Go 1.21) with a JSON handler, so every log line is a machine-readable JSON object. Each warning includes the Pub/Sub `msg_id` and the full `error` as discrete fields, making it straightforward to filter and alert on error classes in any log aggregation system (Datadog, Cloud Logging, etc.).

**Prometheus metrics**
The processor exposes a `/metrics` endpoint (Prometheus scrape format) and a `/healthz` liveness probe on port `8080`, served in a background goroutine alongside the main receive loop.

**Graceful shutdown**
`signal.NotifyContext` cancels the root context on `SIGINT`/`SIGTERM`, causing `sub.Receive` to drain all in-flight `HandleMessage` calls before returning.

### File Structure

```
cmd/processor/
  main.go           — entry point: flags, PubSub/metrics wiring, graceful shutdown
  Dockerfile        — multi-stage build (CGO_ENABLED=0, Alpine runtime)
pkg/store/
  store.go          — Store interface + ScanRecord type
  sqlite.go         — SQLite implementation (WAL, busy timeout, conditional upsert)
  postgres.go       — PostgreSQL implementation (pgxpool, conditional upsert)
  memory.go         — In-memory implementation (testing & local dev)
pkg/processor/
  processor.go      — HandleMessage, error classification, V1/V2 parsing, Upsert
pkg/metrics/
  metrics.go        — Prometheus Recorder (counters + histogram)
pkg/publish/
  publish.go        - Publish for Pub/Sub topic (DQL)
k8s/
  pubsub.yaml       — Pub/Sub emulator Deployment + Service
  pubsub-init.yaml  — Job: creates topic + subscription + dql topic
  postgres.yaml     — PostgreSQL StatefulSet + Service + PVC
  scanner.yaml      — Scanner Deployment
  processor.yaml    — Processor Deployment + Service (probes, Prometheus annotations)
  prometheus.yaml   — Prometheus Deployment + Service + ConfigMap
  grafana.yaml      — Grafana Deployment + Service + ConfigMaps
Tiltfile            — orchestrates the full K8s dev stack (tilt up)
```

### Store Backends

The processor supports multiple storage backends, selected via the `-store` flag:

| Backend | Flag | Best for |
|---------|------|----------|
| SQLite | `-store=sqlite -db=/data/scans.db` | Single-replica, local development, zero-dependency setup |
| PostgreSQL | `-store=postgres` + `POSTGRES_URL` env | Multi-replica, production, high write concurrency |
| Memory | `none` | Example of ability to easily implement new DB stores |

The default is `sqlite` for backward compatibility. Docker-compose and Kubernetes are pre-configured to use PostgreSQL.

### Database Schema

All store backends maintain the same logical schema — one row per unique `(ip, port, service)` tuple:

```sql
CREATE TABLE scan_records (
    ip           TEXT    NOT NULL,
    port         INTEGER NOT NULL,
    service      TEXT    NOT NULL,
    last_scanned BIGINT  NOT NULL,   -- Unix timestamp (SQLite uses INTEGER, which is 64-bit)
    response     TEXT    NOT NULL,
    PRIMARY KEY (ip, port, service)
);
```

The composite primary key `(ip, port, service)` ensures exactly one row per scan target. All access is by primary key — no secondary indexes are needed.

**Conditional upsert** — the core of out-of-order protection:

```sql
INSERT INTO scan_records (ip, port, service, last_scanned, response)
VALUES (...)
ON CONFLICT (ip, port, service) DO UPDATE SET
    last_scanned = EXCLUDED.last_scanned,
    response     = EXCLUDED.response
WHERE EXCLUDED.last_scanned > scan_records.last_scanned;
```

The `WHERE` clause ensures only strictly newer scans overwrite existing records. A 24-hour-old message arriving late is silently ignored at the database level — no read-before-write, no application-level locking, no race between concurrent processors. This single atomic statement handles idempotency (at-least-once delivery) and out-of-order protection simultaneously.

---

## Development

A `Makefile` provides shortcuts that mirror the CI jobs exactly:

| Command | What it runs |
|---------|-------------|
| `make test` | `go test -race ./...` (Postgres tests skipped when `POSTGRES_URL` is unset) |
| `make test-integration` | Same, but sets `POSTGRES_URL` for a local Postgres on `:5432` |
| `make lint` | `golangci-lint run ./...` |
| `make build` | `CGO_ENABLED=0 go build` for both binaries |
| `make tidy` | `go mod tidy && go mod verify` |
| `make up` | `tilt up` |
| `make down` | `tilt down` |

`make lint` requires [golangci-lint](https://golangci-lint.run/docs/welcome/install/local/) to be installed locally. `brew install golangci-lint` The linter config lives in `.golangci.yml`.

### Kubernetes Development

The project can also run on a local Kubernetes cluster using [Tilt](https://tilt.dev/) for easier local dev.

**Prerequisites:**

- Docker Desktop with Kubernetes enabled (Settings → Kubernetes → Enable Kubernetes)
- [Tilt](https://docs.tilt.dev/install.html) installed (`brew install tilt-dev/tap/tilt`)

Tilt builds both container images from the existing Dockerfiles, deploys all Kubernetes manifests, and opens a browser UI showing real-time status for every resource. File changes trigger automatic image rebuilds and pod restarts.

`tilt up`/`tilt down`

---

## Testing

### Integration test

```bash
docker compose up --build
```

This starts the Pub/Sub emulator, creates the topic and subscription, PostgreSQL, and builds and runs the scanner (one scan/second) and the processor. Data is persisted in a named Docker volume (`pg-data`).

To inspect the live state of the database while the stack is running:

```bash
docker compose exec postgres psql -U processor -d scans -c \
  "SELECT ip, port, service,
          to_timestamp(last_scanned) AS scanned_at, response
   FROM scan_records ORDER BY last_scanned DESC LIMIT 10;"
```

You should see one row per unique `(ip, port, service)` tuple, with `last_scanned` advancing forward over time as newer scans arrive.

### CI Tests

Pipelines include automated testing steps listed below in CI Pipeline section

### Commands

**Run tests**

Using the makefile you can run `make test` for simple unit testing or with a local postgresdb running, `make test-integration` to run all tests including postgres

**Publish Valid V2 Message**

```bash
curl -X POST http://localhost:8085/v1/projects/test-project/topics/scan-topic:publish \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [{
      "data": "'$(echo -n '{"ip":"1.2.3.4","port":443,"service":"HTTPS","timestamp":1709654400,"data_version":2,"data":{"response_str":"hello world"}}' | base64)'"
    }]
  }'
```

**Publish Valid V1 Message**

```bash
curl -X POST http://localhost:8085/v1/projects/test-project/topics/scan-topic:publish \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [{
      "data": "'$(echo -n '{"ip":"192.168.1.1","port":80,"service":"HTTP","timestamp":1709654400,"data_version":1,"data":{"response_bytes_utf8":"aGVsbG8gd29ybGQ="}}' | base64)'"
    }]
  }'
```

**Malformed message**

```bash
curl -X POST http://localhost:8085/v1/projects/test-project/topics/scan-topic:publish \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [{
      "data": "'$(echo -n 'not-json' | base64)'"
    }]
  }'
```

**Unknown data_version**

```bash
curl -X POST http://localhost:8085/v1/projects/test-project/topics/scan-topic:publish \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [{
      "data": "'$(echo -n '{"ip":"9.9.9.9","port":22,"service":"SSH","timestamp":1709654400,"data_version":99,"data":{}}' | base64)'"
    }]
  }'
```

---

## Observability

### Grafana Dashboard

The stack includes Prometheus and Grafana with a pre-built dashboard.
`Note I've not used Grafana very much, the dashboard could definitely be improved`

Once `docker compose up --build` is running:

1. Open **<http://localhost:3000>** and log in with `admin` / `admin`
2. Navigate to **Dashboards → Scan Processor**

The dashboard has five panels:

| Panel | What it shows |
|-------|---------------|
| **Message Throughput** | Successful messages processed per second |
| **Error Rate** | Permanent (bad messages) and transient (store errors) error rates |
| **Processing Latency** | p50 / p95 / p99 end-to-end handling time |
| **Message Rate by Status** | Stacked view of all three status labels over time |
| **DLQ Publish Rate** | Rate of messages published to DLQ |

The Prometheus UI is also available at **[http://localhost:9090](http://localhost:9090)** for ad-hoc queries.

---

## CI Pipeline

The repo ships a GitHub Actions workflow (`.github/workflows/ci.yml`) that runs on every push and pull request:

| Job | What it does |
|-----|-------------|
| **lint** | `go mod tidy` drift check + golangci-lint |
| **test** | `go test -race ./...` with Postgres service container + per-package coverage summary |
| **build** | Compiles both binaries and builds both Docker images |

`Currently build does not push anything to image registries`
