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
| `DEST_S3_ADDRESSING_STYLE` | no | `virtual` | `auto`, `virtual`, or `path`; set `path` for S3-compatible stores that have no per-bucket virtual-host DNS, where the default `bucket.endpoint` form redirects; other values fail fast at startup |
| `DEST_S3_REGION` | no | `us-east-1` | |

## Behavior

| Variable | Default | Notes |
|---|---|---|
| `POLL_INTERVAL_SECONDS` | `300` | Post-cycle delay; together with the worst-case cycle duration it must be comfortably below the shortest `ttlStrategy` in effect, or completed runs can be deleted before they are ever seen — see [`ttl-and-observation-windows.md`](ttl-and-observation-windows.md) |
| `RUN_RETENTION_DAYS` | `7` | How long a run stays in `runs.parquet` after it was **last observed**, not after it started |
| `HTTP_TIMEOUT_SECONDS` | `10` | per API request |
| `LIST_PAGE_SIZE` | `500` | Kubernetes list page size; the exporter follows `continue` tokens to the end |
| `HEALTH_PORT` | `8080` | `GET /health`; see the [health endpoint contract](../../README.md#health-endpoint) for response fields and status codes |
| `LOG_LEVEL` | `INFO` | Python logging level: `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL` (case-insensitive; `WARN` and `FATAL` are accepted aliases) |
| `VERSION_FILE` | `VERSION` | read once at startup, reported in `meta.json` |

All numeric variables must parse as integers greater than zero.

## Failure behavior

- **One cluster unreachable** — that cluster contributes no rows and is
  reported `"ok": false` in `meta.json`. Every other cluster is collected and
  written as normal. The new `workflows.parquet` contains only rows from
  successful complete listings; rows from this cluster's previous successful
  snapshot are not carried forward. Its existing ledger rows are untouched and
  expire on the usual retention schedule.
- **A cluster removed from `CLUSTERS_JSON`** — this is a configuration change,
  not evidence that its runs should be deleted. The removed name disappears
  from new snapshots and `meta.json`, while its existing `runs.parquet` rows
  remain unchanged and expire naturally `RUN_RETENTION_DAYS` after their
  `last_seen_at`. There is no early purge or removed-cluster annotation, so
  consumers should expect those historical rows under the old name until
  normal retention removes them.
- **A cluster renamed in `CLUSTERS_JSON`** — cluster name is part of the
  ledger key, so a rename is not treated as an identity migration. Rows under
  the old name remain until their normal retention expiry, and observations
  under the new name accumulate as new `(cluster, uid)` identities. Consumers
  must not combine the names unless they maintain that aliasing themselves.
- **A partial listing** (page 2 of 3 fails) is treated as a failure for that
  cluster, not as a short list. Items already received are discarded along with
  that cluster's prior snapshot rows. A consumer reads a missing workflow as a
  deleted one, so half an answer would show runs vanishing that are still
  there.
- **No cluster listing completes** — nothing is written at all, whether every
  cluster is unreachable or every listing is incomplete. `meta.json` keeps its
  previous `generated_at`, which is what makes the outage visible downstream.
- **Any other exception** is logged with a traceback and the loop waits the
  configured post-cycle delay before continuing. The health heartbeat advances only after a cycle publishes
  all three output objects; once it is two polling intervals old, `/health`
  returns 503 until a later cycle succeeds.

Every failed cycle emits an error record with `failure_phase` (`read`,
`compute`, or `publish`), `affected_clusters`, and `publication`. The latter
is `skipped` when no output object was written, or `partial` with the names of
the objects written and still skipped when an upload failed partway through.
An all-unavailable collection is a read-phase failure with publication
skipped; a partial cluster collection that still publishes is logged by the
collector with the unavailable cluster names and is not treated as a failed
publication.

See [`output-schema.md`](output-schema.md#consumer-contract) for the required
consumer handling of fresh, partial, and unavailable snapshots.
