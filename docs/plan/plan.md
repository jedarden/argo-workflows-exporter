# argo-workflows-exporter Plan

## Overview

Poll the Kubernetes API of any number of clusters for Argo `Workflow`
objects and write their state as Parquet to an S3-compatible bucket, so that
a static dashboard — or any query engine — can read pipeline state without
credentials, without a database, and without the Argo API server.

## Constraints

1. **Read-only, always.** The exporter never mutates anything in a watched
   cluster. Its only write is to its own S3 prefix.
2. **No infrastructure assumptions.** Cluster names, endpoints, namespaces,
   bucket, prefix and credentials all arrive as environment variables. The
   image is portable to any Argo installation.
3. **A live listing is a biased sample.** Argo's `ttlStrategy` deletes
   completed workflows, normally faster for successes than failures, so a
   snapshot cannot support any historical or rate-based question. This drives
   the whole output design — see
   [`../notes/ttl-and-observation-windows.md`](../notes/ttl-and-observation-windows.md).
4. **Degrade partially, never silently.** One unreachable cluster must not
   stop the others, and must not look like a cluster with no workflows.
5. **Long-lived loop**, not a one-shot job — one process, one interval, no
   external scheduler.

## Architecture

```
                 ┌───────────── exporter pod ─────────────┐
  cluster A ───▶ │  k8s_api ─▶ workflows ─▶ ledger ─▶ s3io │ ───▶ S3 bucket
  cluster B ───▶ │                 (poll loop, N seconds)  │      argo/data/
  cluster C ───▶ │                              health :8080│
                 └────────────────────────────────────────┘
```

Each cluster is reached either through the pod's own ServiceAccount (at most
one such entry) or over an unauthenticated read-only API proxy, on whatever
private network path resolves from the pod. Nothing else is dialed.

## Components

| Module | Responsibility |
|---|---|
| `config.py` | Load and validate every environment variable once, at startup. Fail fast with a specific message. |
| `k8s_api.py` | One uniform `GET` across local and proxied clusters; paginated listing that reports whether it completed. |
| `workflows.py` | Turn raw `Workflow` objects into flat rows: template, trigger, phase, duration, failing step. |
| `ledger.py` | Fold each cycle's observations into the durable run record; expire by last-seen. |
| `parquet_io.py` | Schemas, Parquet encode/decode, and schema conformance for records written by an older release. |
| `s3io.py` | Client construction, get/put. |
| `main.py` | Poll loop, cycle orchestration, health endpoint, signal handling. |

## Data models

Two tables and a sidecar, defined in
[`../notes/output-schema.md`](../notes/output-schema.md):

- `workflows.parquet` — what exists right now. Answers "what is running", and
  nothing historical.
- `runs.parquet` — the ledger, keyed by `uid`, outliving the objects it came
  from. Answers everything historical.
- `meta.json` — version, `generated_at` heartbeat, per-cluster reachability,
  row counts.

## Implementation phases

- [x] **Phase 1: Collector.** Config, paginated read-only API access,
  extraction, ledger, Parquet output, S3 upload, health endpoint, container
  image, unit tests, docs. Verified end-to-end against a live Argo
  installation (64 workflows, real extraction, real Parquet round-trip).
- [x] **Phase 2: Deployment.** Container build pipeline, Deployment manifest
  with the S3 credential supplied by reference, and a first consumer reading
  the output. Shipped 2026-08-11 (`ronaldraygun/argo-workflows-exporter:0.1.0`
  on ardenone-cluster, consumed by `dashboard.ardenone.com/argo/`).
- [ ] **Phase 3a: Failure taxonomy (committed 2026-09-01).** `runs.parquet`
  carries `failed_step_message` as a raw string, so failures cannot be
  aggregated. Add `failure_fingerprint`: the message normalized by stripping
  hashes, UUIDs, timestamps, durations, pod names and absolute paths, then
  hashed to a short stable id, plus `failure_class` from a small reviewed
  rule table (timeout, OOM, clone/auth, image pull, test failure, lint,
  build, infrastructure, unknown). Both columns are additive; the raw
  message stays. This is one of the three join sources for the factory
  attempt ledger (NEEDLE plan section 4.4): a bead's CI run is joined on the
  workflow name/commit recorded in `attempt.resolved`, and its fingerprint
  becomes the CI half of a failure signature.
- [ ] **Phase 3b: Depth, if wanted.** Candidates, none committed:
  `workflowtemplates` / `cronworkflows` inventory so templates that have
  never run are visible; per-step rows rather than just the first failure;
  decompressing `status.compressedNodes`; queue-time (`created_at` to
  `started_at`) as a first-class column for scheduling pressure.

## Open questions

- **Retention default.** Seven days is a guess that suits a CI-shaped
  workload. The ledger grows with (runs per day x retention), and the file is
  rewritten whole each cycle, so a very high-volume installation will want to
  revisit both the default and the rewrite-in-place approach.
- **Rewrite vs. append.** Rewriting the whole ledger every cycle is simple,
  atomic per object, and fine at thousands of rows. Partitioning by day would
  scale further at the cost of a consumer having to read many files.
- **Terminal-state stability.** A run's row is updated on every observation,
  including after it reached a terminal phase. This is harmless but does
  rewrite unchanged data; only worth changing if ledger size becomes a
  problem.
