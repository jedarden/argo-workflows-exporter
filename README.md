# argo-workflows-exporter

Polls the Kubernetes API of any number of clusters for Argo `Workflow`
objects and writes their state as Parquet to any S3-compatible bucket. Runs
as a long-lived loop — polls on an interval and re-uploads every cycle.

Nothing about any particular installation is hardcoded: the clusters it polls,
their namespaces, and any remote proxy URLs are supplied at deploy time
through environment variables. Cluster and S3 access use different credential
sources: a local cluster uses this pod's in-cluster Kubernetes endpoint and
projected ServiceAccount files, a remote cluster uses an unauthenticated API
proxy, and the S3 destination uses the `DEST_S3_*` endpoint and credential
variables. See
[`docs/notes/configuration.md`](docs/notes/configuration.md).

## Why it keeps its own record

A live listing of `Workflow` objects is not a record of what ran. Argo's
`ttlStrategy` deletes completed workflows — often within minutes, and
commonly **sooner for successes than for failures**. A snapshot taken at any
moment is therefore biased towards whatever fails and lingers, and a cluster
whose runs are mostly green can present a listing that is mostly red.

So this exporter writes two things: a snapshot of what exists right now, and
a **run ledger** that remembers each run past the deletion of the object it
came from. Anything resembling a success rate, a duration trend or a failure
count has to be read from the ledger.

The ledger's accuracy depends on the effective polling cadence fitting inside
the shortest TTL in effect: the post-cycle delay plus the worst-case cycle
duration must be shorter than that TTL. A run that starts and is deleted
between two polls is never seen.
[`docs/notes/ttl-and-observation-windows.md`](docs/notes/ttl-and-observation-windows.md)
works through how to choose the interval.

## Structure

- `src/` — the exporter itself
- `docs/notes/` — features, constraints, design decisions
- `docs/operations/` — production deployment, rollback, and upgrade runbooks
- `docs/research/` — external reference material and prior art
- `docs/plan/plan.md` — application plan with shipped phases and tracked follow-ups

The checked phases in the plan identify the collector and deployment behavior
that has shipped; they do not imply that every later operator-documentation or
consumer-contract hardening task is closed. The plan's follow-up table is the
authoritative status for that remaining work.

## Output

Three objects are written under `DEST_S3_PREFIX` (default `argo/data`) after
every cycle in which at least one cluster completes its listing:

- `workflows.parquet` — one row per `Workflow` object that currently exists in
  each cluster whose listing completed, overwritten each cycle
- `runs.parquet` — the ledger: one row per run ever observed, updated in
  place as a run progresses and retained `RUN_RETENTION_DAYS` past the last
  time it was seen
- `meta.json` — provenance sidecar: version, generation timestamp, per-cluster
  reachability, and row counts. Written **last**, as the commit marker for
  the cycle.

S3 has no multi-object write, so a cycle's three objects are not published
atomically. Every object of a cycle carries the same `generation_id` — in
`meta.json` and in each Parquet file's metadata — so a consumer can detect a
publication that was torn partway by a failed upload and hold its last
complete generation instead of mixing two cycles' data. The write ordering,
retry behavior, and the state each failure leaves behind are specified in
[`docs/notes/atomic-publication.md`](docs/notes/atomic-publication.md).

An unreachable cluster, or one whose listing fails partway through pagination,
contributes no rows. Its rows from the previous successful snapshot are **not**
carried into the new `workflows.parquet`; `meta.json` marks that cluster
`"ok": false` so consumers do not mistake unavailable data for deletion.
Column definitions and the storage-level consumer contract are in
[`docs/notes/output-schema.md`](docs/notes/output-schema.md). The callable
Python API for loading, selecting, normalizing, and interpreting a publication
is documented in
[`docs/notes/consumer-api.md`](docs/notes/consumer-api.md).

The failure taxonomy is shipped, not a planned schema addition:
`failure_fingerprint` and `failure_class` first became available in exporter
`0.2.0` (`264dae7be0e59176f106d87c7a42ea588c4fcf51`). The shared derivation
used by both Parquet outputs was then consolidated in `0.2.44`
(`163283f3cf35a1a6699fbe8f7b08f43fa9ce484c`). Consumers must still accept null
values for rows written before `0.2.0`; schema conformance does not invent a
taxonomy for a run that has not been observed again.

A cycle in which **no cluster completes its listing** writes nothing at all,
rather than replacing good data with an empty snapshot. `meta.json`'s
`generated_at` going stale is the signal that collection has stopped. A
consumer derives staleness from the effective cadence, not a fixed timeout:
`age >= C_max + meta.poll_interval_seconds`, where `C_max` is its configured
upper bound for one exporter cycle. It must alert, retain the last complete
generation as last-known data, and never present that retained generation as
current; generation-id agreement alone does not make a frozen heartbeat fresh.

`generated_at` is captured once at cycle start in UTC RFC 3339 second-resolution
form and reused as the `generation_id` timestamp prefix and row observation
instant. It is committed only when the final `meta.json` upload succeeds, so a
slow cycle's runtime contributes to age and a failed publication cannot advance
the consumer heartbeat.

## Access required

Read-only, on every cluster polled:

```yaml
- apiGroups: ["argoproj.io"]
  resources: ["workflows"]
  verbs: ["get", "list"]
```

`watch` is deliberately not requested. The exporter is a periodic list
client: each cycle it GETs the collection endpoint and pages through it with
`limit`/`continue`. It never opens a watch stream, so the `watch` verb would
grant nothing it can use. Freshness comes from polling faster than the
shortest `ttlStrategy` window in effect, not from an event stream — see
[`docs/notes/ttl-and-observation-windows.md`](docs/notes/ttl-and-observation-windows.md).

Cluster access has two mutually exclusive modes:

- A local entry has no `base_url`. It uses the in-cluster Kubernetes endpoint
  (`https://kubernetes.default.svc`) and this pod's projected
  ServiceAccount token and CA files at
  `/var/run/secrets/kubernetes.io/serviceaccount/token` and
  `/var/run/secrets/kubernetes.io/serviceaccount/ca.crt`. These local cluster
  credentials are not environment variables.
- A proxied entry has a `base_url`. It calls that read-only API proxy without
  sending a Kubernetes credential; the URL must be resolvable from the pod
  over the private network path provided by the deployment. The exporter
  never holds a credential of its own for a remote cluster.

The S3 destination is separate from cluster authentication. Its endpoint,
access key, secret key, bucket, and optional region/prefix are supplied with
the `DEST_S3_*` environment variables.

## Usage

No public container image is published yet. Build the image from this checkout
before running it:

```bash
docker build -t argo-workflows-exporter:0.2.0 .

docker run --rm \
  -e CLUSTERS_JSON='[{"name":"ci","base_url":"http://kubectl-proxy.example:8001"}]' \
  -e WORKFLOW_NAMESPACE=argo \
  -e DEST_S3_ENDPOINT=https://s3.example.com \
  -e DEST_S3_ACCESS_KEY_ID=... \
  -e DEST_S3_SECRET_ACCESS_KEY=... \
  -e DEST_S3_BUCKET=your-bucket \
  -e DEST_S3_PREFIX=argo/data \
  argo-workflows-exporter:0.2.0
```

### Health endpoint

`GET /health` listens on port 8080 by default; set `HEALTH_PORT` to change it.
Every response is compact JSON with `Content-Type: application/json` and
`Cache-Control: no-store`.

| Condition | HTTP status | Response body |
|---|---:|---|
| No cycle has completed successfully yet | 503 | `{"status":"starting","last_success_at":null}` |
| The last success is younger than two polling intervals | 200 | `{"status":"ok","last_success_at":"2026-01-01T00:00:00Z","age_seconds":12.345}` |
| The last success is at least two polling intervals old | 503 | `{"status":"stale","last_success_at":"2026-01-01T00:00:00Z","age_seconds":600.0}` |

`status` is `starting`, `ok`, or `stale`. `last_success_at` is `null` during
startup and otherwise records the successful cycle's completion time in UTC to
whole-second precision. `age_seconds` is a nonnegative number rounded to three
decimal places; it is omitted while startup has not yet completed a cycle.

A cycle updates the heartbeat only after it publishes all three output objects.
A failed cycle does not advance the timestamp, so the endpoint becomes `stale`
when the original success reaches `2 * POLL_INTERVAL_SECONDS` and remains
unhealthy until a later cycle succeeds. A later success immediately returns the
endpoint to HTTP 200.

`POLL_INTERVAL_SECONDS` is the delay after each cycle returns, not a
start-to-start deadline. The loop runs one cycle at a time, waits that delay
after a success or failure, and then starts the next cycle. A cycle that takes
longer than the configured interval therefore lengthens the effective cadence;
it is never overlapped or immediately caught up.

## Development

```bash
pip install -r requirements-dev.txt
python -m pytest tests/ -q
```

To verify the built image and the live GitOps Deployment contract (including
the referenced S3 secret and `/health` probes), run this from a checkout that
also has `declarative-config` available:

```bash
tests/container-packaging/run.sh
```

Set `DECLARATIVE_CONFIG_DIR` when that checkout is elsewhere. The check builds
the Deployment's semver-pinned image tag locally, starts it with non-secret
loopback test values, and removes its temporary container and image afterward.

The production image release, GitOps rollout, rollback, RBAC, probe, and
schema/generation upgrade procedures are in the
[`production runbook`](docs/operations/production-runbook.md).

## License

MIT — see [LICENSE](LICENSE).
