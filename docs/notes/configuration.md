# Configuration

All configuration is environment variables, loaded and validated once at
startup in `src/config.py`. A missing or malformed required variable fails
fast with a clear message rather than crash-looping later mid-cycle.

## `CLUSTERS_JSON` (required)

A JSON array describing every cluster to poll.

```json
[
  {"name": "ci", "base_url": "http://kubectl-proxy-ci.example:8001"},
  {"name": "staging", "base_url": "http://kubectl-proxy-staging.example:8001",
   "namespace": "argo-workflows"},
  {"name": "here"}
]
```

Fields per entry:

- `name` (required) — how this cluster is labeled in every output row. Must be
  unique across the array.
- `base_url` (optional) — the read-only API proxy's base URL, reached from
  inside the pod with no authentication. Omit it on the cluster the exporter
  itself runs in: that entry is reached through the pod's own ServiceAccount
  against `https://kubernetes.default.svc` instead.
- `namespace` (optional) — overrides `WORKFLOW_NAMESPACE` for this cluster
  only, for installations that put Argo in a different namespace on different
  clusters.

**Zero local entries is normal.** The exporter commonly runs somewhere other
than the cluster it watches and reaches every target over a proxy. More than
one entry without a `base_url` is rejected — "local" means this pod's own
ServiceAccount, and there is only one of those.

## Scope

| Variable | Default | Notes |
|---|---|---|
| `WORKFLOW_NAMESPACE` | `` (all) | Empty polls the cluster-scoped list endpoint and returns workflows from every namespace. Set it to restrict collection, and to keep the required RBAC namespaced. |

## Destination (S3-compatible)

| Variable | Required | Default | Notes |
|---|---|---|---|
| `DEST_S3_ENDPOINT` | yes | — | |
| `DEST_S3_ACCESS_KEY_ID` | yes | — | |
| `DEST_S3_SECRET_ACCESS_KEY` | yes | — | |
| `DEST_S3_BUCKET` | yes | — | |
| `DEST_S3_PREFIX` | no | `argo/data` | key prefix for all three output objects |
| `DEST_S3_ADDRESSING_STYLE` | no | `virtual` | set `path` for S3-compatible stores that have no per-bucket virtual-host DNS, where the default `bucket.endpoint` form redirects |
| `DEST_S3_REGION` | no | `us-east-1` | |

## Behavior

| Variable | Default | Notes |
|---|---|---|
| `POLL_INTERVAL_SECONDS` | `300` | Must be comfortably below the shortest `ttlStrategy` in effect on the clusters polled, or completed runs are deleted before they are ever seen — see [`ttl-and-observation-windows.md`](ttl-and-observation-windows.md) |
| `RUN_RETENTION_DAYS` | `7` | How long a run stays in `runs.parquet` after it was **last observed**, not after it started |
| `HTTP_TIMEOUT_SECONDS` | `10` | per API request |
| `LIST_PAGE_SIZE` | `500` | Kubernetes list page size; the exporter follows `continue` tokens to the end |
| `HEALTH_PORT` | `8080` | `GET /health` |
| `LOG_LEVEL` | `INFO` | |
| `VERSION_FILE` | `VERSION` | read once at startup, reported in `meta.json` |

All numeric variables must parse as integers greater than zero.

## Failure behavior

- **One cluster unreachable** — that cluster contributes no rows and is
  reported `"ok": false` in `meta.json`. Every other cluster is collected and
  written as normal. Its existing ledger rows are untouched and expire on the
  usual retention schedule.
- **A partial listing** (page 2 of 3 fails) is treated as a failure for that
  cluster, not as a short list. A consumer reads a missing workflow as a
  deleted one, so half an answer would show runs vanishing that are still
  there.
- **No cluster reachable** — nothing is written at all. `meta.json` keeps its
  previous `generated_at`, which is what makes the outage visible downstream.
- **Any other exception** is logged with a traceback and the loop continues to
  the next interval.
