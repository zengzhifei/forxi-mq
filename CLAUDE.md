# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

```bash
make build       # build frontend (npm) + Go packages
make test        # run all tests (requires Redis on localhost:6379)
make vet         # go vet
make frontend    # build Vue dashboard only (dashboard/ui → dist/)
```

Tests are integration tests — they need a running Redis. No unit tests with mocks.

## Architecture

forxi-mq is a lightweight message queue on Redis Streams. Single Go module (`github.com/zengzhifei/forxi-mq`), one external dependency (`go-redis/v9`).

**Entry point**: `engine.go` — `NewEngine(redisAddr, group, opts...)` creates the Engine that wires all components. Functional options pattern for configuration. `mq/config.go` owns defaults and validation.

**Message flow**:

```
Publish → Redis Stream → Consumer Group (XREADGROUP ">")
  ↓ fail (leave in PEL)
Recovery (XAUTOCLAIM) → retry counter ++
  ├─ under maxRetry → retryCh → consumer reprocesses
  └─ over maxRetry  → DLQ (marked dead, stays in PEL for manual requeue)
```

**Delay flow**: ZSET (score=dueTime) + Hash (body) → Lua script atomically pops due items → XADD to Stream. A delay-map key (`fxmq:delay-map:{topic}:{delayID}`) stores the resulting stream ID for tracking.

**Key components**:
- `consumer/` — Worker pool per topic. Workers read new messages (`>`); a single `retryWorker` processes recovered messages from a channel (avoids resetting idle time with XREADGROUP "0"). Handler runs under a context timeout.
- `recovery/` — Periodically XAUTOCLAIMs timed-out pending messages, increments retry counter, routes to retry reprocessing or DLQ. Also cleans stale consumers (no pending, idle > threshold).
- `delay/` — Polls per-topic delay ZSETs, runs Lua to transfer due messages to Stream.
- `retry/` — Per-message-per-group retry counter in Redis keys (`fxmq:retry:{topic}:{group}:{msgID}`). Value `-1` marks dead.
- `deadletter/` — DLQ is a per-group Redis Stream (`fxmq:dead:{topic}:{group}`). Retention trimmer periodically cleans expired DLQ entries, ACKs corresponding PEL entries, and deletes retry counters.
- `alert/` — Periodic metric check (lag/pending/dead), webhook to Feishu/DingTalk/WeCom with cooldown.
- `dashboard/` — Vue 3 + Element Plus SPA, embedded via `//go:embed`, served by Go net/http.
- `log/` — Minimal `Logger` interface (Info/Warn/Error/Debug), default wraps `log/slog` as JSON to stderr. Accepts custom implementations.
- `internal/` — Redis key naming: `fxmq:{topic}`, `fxmq:dead:{topic}:{group}`, `fxmq:delay:{topic}`, etc.

**Important design decisions**:
- Messages are **never re-published** (no duplicate XADD). XAUTOCLAIM transfers ownership of the existing PEL entry. Message ID stays constant through the entire retry lifecycle.
- Dead messages are **not ACKed from PEL** — they stay in PEL for potential manual requeue from the dashboard.
- Consumer name defaults to `$HOSTNAME` for automatic horizontal scaling identity.
- The `retryCh` channel (buffer 100) decouples recovery from consumption. If full, message stays in PEL for next recovery cycle.
