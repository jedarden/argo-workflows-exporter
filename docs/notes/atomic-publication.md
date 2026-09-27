# Atomic publication

A cycle publishes three objects to one S3 prefix — `workflows.parquet`,
`runs.parquet`, `meta.json` — and S3 has no multi-object write. Each object
is an independent PUT, so a generation can never appear atomically: there is
always a window, and a failed upload, in which the stored objects do not all
belong to the same cycle. The design here does not pretend otherwise. It
makes the window as small as the read-modify-write allows, orders the writes
so that the object consumers read first is the one that commits the
generation, and stamps every object with an identity that makes a torn set
detectable rather than silently wrong.

## Generation identity

Every cycle generates a `generation_id`:

```
<generated_at>-<12 hex characters>
```

for example `2026-09-23T19:00:00Z-3f9c2a1b7d44`. The timestamp prefix keeps
the id human-readable and lexicographically sorted by time; the random
suffix exists because `generated_at` has only second resolution and the poll
interval is operator-configured — two cycles that ever land in the same
second must still be distinguishable.

### Timestamp semantics

`generated_at` is the cycle-start instant. The exporter captures it once,
before the first Kubernetes or S3 operation, and reuses that exact UTC
second-resolution value for the `generation_id` prefix, every row's
`observed_at`/`last_seen_at`, and the sidecar heartbeat. It is not the time
computation finishes and not the time an upload request returns. This keeps
all timestamps in one generation comparable even when collection or
serialization is slow.

The timestamp becomes visible only when the `meta.json` commit marker is
successfully uploaded. A read or compute failure writes no object; a failed
data or marker upload leaves the previous sidecar's `generated_at` in place.
If a failed upload leaves newer Parquet objects beside that marker, their
newer id prefix is a torn-publication signal, not a new heartbeat. Consumers
calculate age from the committed sidecar `generated_at`, with the generation
id prefix required to match it.

All three objects of a cycle carry the same id:

- `workflows.parquet` and `runs.parquet` — as file-level Parquet
  key-value metadata under the key `generation_id`. The id lives in the file
  metadata, not in the rows, so **an empty snapshot is still a generation**:
  a zero-row snapshot can be paired with its `meta.json` exactly like a full
  one. Reading the id needs only the Parquet footer
  (`pyarrow.parquet.read_schema`, or the equivalent footer-only read on any
  other client), not the whole object.
- `meta.json` — as a top-level `"generation_id"` field.

Objects from the release before generation identity was introduced are a
documented compatibility case: their Parquet footers have no `generation_id`
key. A consumer may still decode those rows for an explicit historical
migration, but it cannot treat them as a complete current publication. The
footer check returns no id and the consumer holds its last paired generation;
it must not invent an id or treat the absence as matching any sidecar. This
is stricter than the valid zero-row case, where the rows are empty but the
footer still carries the generation id.

The producer handles a footerless `runs.parquet` differently from a consumer:
it treats the file as reusable ledger history. The read path conforms its rows
to the current `RUNS_SCHEMA` without consulting the footer, so retained runs
survive an exporter upgrade even though the old object cannot be presented as
a current publication. The producer never carries an old or missing id
forward. It creates one fresh id at cycle start and stamps that id into the
new `workflows.parquet`, `runs.parquet`, and `meta.json` payloads. If listing,
ledger folding, or encoding fails, the read/compute phase has issued no PUT and
the existing objects remain untouched; if a later PUT fails, the old
`meta.json` remains the commit marker and a pairing-aware consumer retains the
previous complete publication until the next full retry succeeds.

The id names a *publication*, not a content hash. Two consecutive cycles
that observed identical rows publish different ids, and the rows themselves
are unchanged; conversely a consumer must not interpret equality of two
objects' ids as equality of their contents.

## Write ordering

`_run_cycle` runs in three phases:

1. **Read phase.** List every cluster, then download the stored
   `runs.parquet` (absent on a first run — that is the normal `NoSuchKey`
   path, not an error). Nothing is written. A failure here raises before any
   stored object is touched: the previous generation survives intact.
2. **Compute phase.** Fold the observations into the ledger, serialize all
   three payloads in memory. A failure here also leaves storage untouched.
3. **Publish phase.** Upload `workflows.parquet`, then `runs.parquet`, then
   `meta.json`, in that order.

`meta.json` is written **last** because it is the commit marker. It is the
first object every consumer is told to read, and when it lands, everything
it describes — row counts, per-cluster reachability, generation id — is
already in place. Advancing the marker last is the closest thing to a commit
S3 offers: before it, a partial publication is invisible to a consumer that
trusts `meta.json`; after it, the generation is complete.

Because the timestamp is captured at cycle start, a slow but successful cycle
is intentionally published with a non-zero heartbeat age. The consumer's
`C_max` must cover the complete cycle runtime; the effective cadence remains
`C_max + meta.poll_interval_seconds`.

Within the data objects the order is fixed and tested:
`workflows.parquet`, `runs.parquet`, `meta.json`.

The whole cycle is idempotent with respect to itself. Nothing in the read
phase depends on anything the publish phase wrote, and every upload is a PUT
of a complete object under a key that gets fully overwritten, so the cycle
is safe to re-run from the top as many times as it fails.

## Single-writer contract

The read-modify-write of `runs.parquet` assumes that exactly one exporter
process writes a given S3 prefix. This is a deployment invariant, not a lock
provided by S3 or by `generation_id`.

If two processes poll the same clusters and prefix concurrently, both can
download the same old ledger and then upload a different fold of it. The last
`runs.parquet` PUT wins, so observations and `last_seen_at` refreshes from the
other process can be silently lost. Their publish phases can also interleave,
leaving different generation ids across the three objects. Consumers can
detect that latter torn state by checking generation ids, but they cannot
recover ledger rows lost by the concurrent read-modify-write.

The GitOps Deployment therefore enforces this contract with
`replicas: 1` and `strategy.type: Recreate`. Recreate terminates the old pod
before Kubernetes starts its replacement, so a normal update cannot briefly
run old and new exporters against the same prefix. A second Deployment, a
manually started exporter, or scaling this Deployment above one replica is
unsupported when it uses the same prefix. Operators that need independent
writers must give them different prefixes. During the intentional hand-off,
the last complete generation remains available and the replacement resumes by
reading that ledger from S3.

## Retries

Retries exist at two layers, and neither one retries a *publication*:

- **Per request, inside the SDK.** The boto3 client is pinned to
  `retries={"max_attempts": 10, "mode": "standard"}` (in `src/s3io.py`),
  which retries throttling and 5xx errors with exponential backoff, and is
  quota-aware. It is pinned rather than left to the installed botocore's
  default so the transient-failure behavior does not drift between deploys.
  A request that exhausts its attempts raises `ClientError`.
- **Per cycle, in the poll loop.** A failed cycle is not retried
  immediately. The exception propagates out of `_run_cycle`, is logged with
  what was and was not written, and the loop simply runs the next cycle on
  schedule. That is the repair for every failure mode below: a full
  re-publication under a fresh generation id. The health endpoint reflects
  this too — it returns 200 only after a cycle that published all three
  objects, so a pod stuck failing its publish reports 503.

The error record identifies `failure_phase` (`read`, `compute`, or `publish`),
the `affected_clusters`, and `publication`. Publication is `skipped` when
the failure occurred before the first PUT, or `partial` when one or more
objects landed before an upload failed; `published_objects` and
`skipped_objects` name the exact members of the three-object set in either
case.

## What each failure leaves behind

Assume a previous complete generation `G_old` on storage, and a cycle
publishing `G_new`:

| Failure | Objects written before abort | Stored state | Consumer-visible effect |
|---|---|---|---|
| a cluster listing fails (some clusters answered) | none | `G_old` intact | `meta.json`'s `clusters[].ok` marks the failed cluster; its rows are absent from the new snapshot but nothing is torn |
| **every** cluster listing fails | none | `G_old` intact, nothing written | `generated_at` stops advancing; once its age reaches `C_max + meta.poll_interval_seconds`, the consumer heartbeat is stale |
| `runs.parquet` download fails (non-404) | none | `G_old` intact | only staleness; `G_old` is complete and self-consistent |
| `workflows.parquet` PUT fails | none | `G_old` intact | none — the cycle aborted before the first write |
| `runs.parquet` PUT fails | `workflows.parquet` | snapshot `G_new`, ledger and marker `G_old` | **torn**: `meta.json` pairs with the ledger but not the snapshot; detectable, see below |
| `meta.json` PUT fails | both Parquet objects | both data objects `G_new`, marker `G_old` | **torn**: `meta.json` describes a generation whose data objects no longer exist |

The torn states are the interesting ones. In both, the objects on storage
are *readable individually* — each is a complete, valid object from some
cycle — but they are not from the *same* cycle. A consumer that trusts
`meta.json` alone would read `G_old`'s counts beside `G_new`'s snapshot: a
count mismatch at best, and at worst a quiet misreading of the ledger as
being from the same moment as the snapshot.

Generation identity is what makes this a detection instead of a guess. The
consumer contract in [`output-schema.md`](output-schema.md) resolves it:
read `meta.json`, then read only each Parquet's footer and require

```
meta.generation_id
  == workflows.parquet footer generation_id
  == runs.parquet footer generation_id
```

If the ids disagree, the stored set is torn. The right response is to hold
the previously paired generation and retry later — not to mix. Because the
ids sort with time, an id *greater* than `meta.json`'s means data from a
cycle newer than the marker is on storage; the torn publication will be
repaired by the exporter's next successful cycle, which republishes all
three objects under a fresh id. No operator action is required, and the
ledger loses nothing by it: its next merge re-reads whatever ledger object
is stored and re-folds the same observations.

Two things a torn state is **not**:

- It is not silent. `meta.json` is never written to a torn generation, so
  `generated_at` — the freshness heartbeat — always names a cycle whose
  three objects were published together. That only proves consistency, not
  freshness: a complete old generation still becomes stale when its age
  reaches the effective cadence defined in
  [`output-schema.md`](output-schema.md#heartbeat-freshness).
- It is not repaired in place. The exporter does not track which objects of
  a generation landed; it re-runs the whole cycle. Any in-place
  reconciliation would need its own state on storage and would only
  reintroduce the ordering problem it tries to solve.

## Why not stronger mechanisms

- **Per-object versioning or a versioned bucket** gives point-in-time
  reads but no cross-object consistency: listing versions still shows
  objects from different cycles, and picking a coherent set means
  reimplementing the manifest below, in the bucket, with no help from the
  data.
- **Writing to temporary keys and copying** does not help: S3 `CopyObject`
  is still a per-object operation, so the fan-out problem is unchanged and
  the number of writes doubles.
- **Encoding the whole generation in one object** (single Parquet with both
  tables, say) would buy atomicity by making `meta.json` redundant — but it
  couples two outputs with different lifecycles and forces every snapshot
  consumer to download the full history. The three-object layout is a
  deliberate contract; the generation id is the cheapest repair that keeps
  it.

The one thing this design cannot do is make a torn window impossible. What
it does instead is bound the damage: the marker moves last, every object
says which cycle it belongs to, and any consumer that checks the pairing
before mixing objects can never present two cycles' data as one.
