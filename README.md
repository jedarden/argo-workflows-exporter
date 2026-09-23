# argo-workflows-exporter

Polls the Kubernetes API of any number of clusters for Argo `Workflow`
objects and writes their state as Parquet to any S3-compatible bucket. Runs
as a long-lived loop — polls on an interval and re-uploads every cycle.

Nothing about any particular installation is hardcoded: every cluster it
polls, every namespace, every endpoint and every credential is supplied at
deploy time through environment variables. See
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

The ledger's accuracy depends on polling faster than the shortest TTL in
effect — a run that starts and is deleted between two polls is never seen.
[`docs/notes/ttl-and-observation-windows.md`](docs/notes/ttl-and-observation-windows.md)
works through how to choose the interval.

## Structure

- `src/` — the exporter itself
- `docs/notes/` — features, constraints, design decisions
- `docs/research/` — external reference material and prior art
- `docs/plan/plan.md` — complete application plan

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
Column definitions and the full consumer contract are in
[`docs/notes/output-schema.md`](docs/notes/output-schema.md).

A cycle in which **no cluster completes its listing** writes nothing at all,
rather than replacing good data with an empty snapshot. `meta.json`'s
`generated_at` going stale is the signal that collection has stopped.

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

Each cluster is reached either through this pod's own ServiceAccount (the
entry with no `base_url`) or over an unauthenticated read-only API proxy at
`base_url`, on whatever private network path makes that URL resolvable from
the pod. The exporter never needs write access and never holds a cluster
credential of its own for a remote cluster.

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

`GET /health` on port 8080 (configurable) returns 200 once a cycle has
completed successfully, 503 before that.

## Development

```bash
pip install -r requirements-dev.txt
python -m pytest tests/ -q
```

## License

MIT — see [LICENSE](LICENSE).
