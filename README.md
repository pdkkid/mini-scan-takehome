# Mini-Scan

Hello!

As you've heard by now, Censys scans the internet at an incredible scale. Processing the results necessitates scaling horizontally across thousands of machines. One key aspect of our architecture is the use of distributed queues to pass data between machines.

---

The `docker-compose.yml` file sets up a toy example of a scanner. It spins up a Google Pub/Sub emulator, creates a topic and subscription, and publishes scan results to the topic. It can be run via `docker compose up`.

Your job is to build the data processing side. It should:

1.  Pull scan results from the subscription `scan-sub`.
2.  Maintain an up-to-date record of each unique `(ip, port, service)`. This should contain when the service was last scanned and a string containing the service's response.

> ***NOTE***The scanner can publish data in two formats, shown below. In both of the following examples, the service response should be stored as: `"hello world"`.
> 
> ```javascript
> {  // ...  "data_version": 1,  "data": {    "response_bytes_utf8": "aGVsbG8gd29ybGQ="  }}{  // ...  "data_version": 2,  "data": {    "response_str": "hello world"  }}
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

The processor is implemented as a new `cmd/processor` binary. All logic lives in two new packages:

-   **`pkg/store`** — a `Store` interface with a SQLite implementation, making it straightforward to swap in a different backend (e.g. PostgreSQL).
-   **`pkg/processor`** — message parsing and dispatch; calls `store.Upsert` and handles Ack/Nack.

### Design Decisions

**Store interface for swappability**`pkg/store/store.go` defines a `Store` interface with `Upsert` and `Get` methods. Adding a new backend (Postgres, DynamoDB, etc.) requires only implementing that interface — no changes to the processor or main.

**Atomic out-of-order protection**The SQLite upsert uses a single atomic SQL statement:

```sql
INSERT INTO scan_records ...ON CONFLICT(ip, port, service) DO UPDATE SET    last_scanned = excluded.last_scanned,    response     = excluded.responseWHERE excluded.last_scanned > scan_records.last_scanned;
```

The `WHERE` clause on `DO UPDATE` means a 24-hour-old message is silently ignored at the database level — no read-before-write, no application-level locking, no race between two concurrent processors.

**At-least-once semantics**The processor calls `msg.Nack()` on any failure (parse error, store error), so Pub/Sub redelivers the message. `msg.Ack()` is called only after a successful `Upsert`. Combined with the idempotent upsert, redeliveries are harmless.

**V1 / V2 parsing**When `encoding/json` unmarshals into `scanning.Scan`, the `Data interface{}` field becomes `map[string]interface{}`. The processor re-marshals that map back to JSON bytes and unmarshals into the correct typed struct (`V1Data` or `V2Data`) based on `DataVersion`. For V1, Go’s `encoding/json` automatically base64-decodes the `response_bytes_utf8` string into `[]byte`.

**Horizontal scaling**Multiple processor replicas can run against the same Pub/Sub subscription — Pub/Sub load-balances messages across subscribers automatically. The conditional upsert ensures correctness when two replicas race to write the same `(ip, port, service)`. SQLite is configured with WAL journal mode and a 5-second busy timeout to reduce lock contention on a shared volume. To scale beyond a single volume, swap the store for a networked database (the interface makes this a one-file change).

**Graceful shutdown**`signal.NotifyContext` cancels the root context on `SIGINT`/`SIGTERM`. `sub.Receive` drains all in-flight `HandleMessage` calls before returning, ensuring every message is Acked or Nacked cleanly before exit.

### File Structure

```
cmd/processor/  main.go        — entry point: flags, PubSub wiring, graceful shutdown  Dockerfile     — multi-stage build (CGO_ENABLED=0, Alpine runtime)pkg/store/  store.go       — Store interface + ScanRecord type  sqlite.go      — SQLite implementation (WAL, busy timeout, conditional upsert)  sqlite_test.go — unit testspkg/processor/  processor.go      — HandleMessage, V1/V2 parsing, Upsert  processor_test.go — unit tests with an in-memory mock store
```

---

## Testing

### Automated tests

```bash
go test ./pkg/... -v -race
```

Runs 12 unit tests across `pkg/store` and `pkg/processor` with the race detector enabled. No external dependencies required.

### Integration test (full stack)

```bash
docker compose up --build
```

This starts the Pub/Sub emulator, creates the topic and subscription, builds and runs the scanner (one scan/second) and the processor. The SQLite database is stored in a named Docker volume (`scan-data`).

To inspect the live state of the database while the stack is running:

```bash
# Open a shell inside the processor containerdocker compose exec processor sh# Inside the container — query the databaseapk add --no-cache sqlitesqlite3 /data/scans.db "SELECT ip, port, service, datetime(last_scanned, 'unixepoch') AS scanned_at, response FROM scan_records ORDER BY last_scanned DESC;"
```

You should see one row per unique `(ip, port, service)` tuple, with `last_scanned` advancing forward over time as newer scans arrive.