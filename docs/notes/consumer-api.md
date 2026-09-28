# Consumer API and failure contract

`src.consumer` is the reference reader for the three objects published under
`DEST_S3_PREFIX`. It turns independently uploaded S3 objects into one selected
publication, but it does not make S3's multi-object write atomic. A caller must
keep the selected publication from the previous successful read and pass it
back as `last_complete` on the next read.

The canonical loading entry point is:

```python
from src.consumer import read_generation

selected = read_generation(
    s3,
    bucket,
    prefix,
    last_complete=last_complete,
    max_cycle_seconds=C_MAX,
)
```

`load_generation` and `read_publication` are aliases for
`read_generation`. They do not provide different consistency or error
semantics.

## Public types and loading API

### `Publication` / `Generation`

```python
Publication(
    meta: Mapping[str, Any],
    workflows: bytes | None,
    runs: bytes | None,
)
```

`Publication` keeps the decoded `meta.json` mapping and the raw Parquet bytes
together. A candidate being checked may contain `None` for a missing data
object. A publication returned by `read_generation` is either a complete,
paired publication or the caller's previous `last_complete` value; it is
never a mix of candidate and previous bytes. `Generation` is a compatibility
alias for the same type.

### `read_generation`

```python
read_generation(
    s3,
    bucket: str,
    prefix: str,
    last_complete: Publication | None = None,
    *,
    max_cycle_seconds: int | float | None = None,
    now: datetime | None = None,
) -> Publication | None
```

The reader performs this ordered operation:

1. Download `meta.json` first.
2. Decode and validate it with the `meta.json` contract.
3. If freshness checking is enabled, compare its `generated_at` to `now`.
4. Download both Parquet objects.
5. Compare the three generation identities and return the candidate only when
   it is complete and internally consistent.

`now` is intended for deterministic tests. In production, omit it and the
reader uses the current UTC time. Supplying `max_cycle_seconds` enables the
heartbeat check; omitting it checks publication pairing but does not make a
freshness claim.

The reader returns raw bytes so callers can choose the row view they need.
Decode a selected publication with the row helpers below; do not interpret a
candidate's rows before it has been selected.

## Failure behavior

The reader uses `None` to represent an absent S3 object, not an exception. It
returns `last_complete` for a rejected candidate, or `None` when there is no
previous complete publication. `None` means **no current generation is
available**; it does not mean an empty snapshot.

| Storage state | Data downloads | Return value | Consumer meaning |
|---|---|---|---|
| `meta.json` is absent | Marker only | `last_complete` or `None` | With no prior value, bootstrap/no committed generation; with one, retain it only as last-known data and mark current data unavailable. Do not read stray Parquet objects. |
| `meta.json` is invalid JSON or violates the schema | Marker only | `last_complete` or `None` | Reject the candidate; the marker cannot commit data it does not describe. |
| `meta.json` is stale | Marker only | `last_complete` or `None` | Current data is unavailable; alert and, if present, retain the result only as last-known data. |
| A Parquet object is absent | Both data objects are attempted | `last_complete` or `None` | Incomplete publication, not an empty table. |
| A footer is unreadable or has no `generation_id` | Both data objects are attempted | `last_complete` or `None` | Reject the candidate as legacy/corrupt/torn. |
| The three generation ids differ | Both data objects are attempted | `last_complete` or `None` | Reject the torn publication; never mix objects from different cycles. |
| All three ids agree and metadata is valid | Both data objects are complete | Candidate | A complete publication; zero-row Parquet files are valid. |
| A non-missing S3 error occurs | As far as the error occurs | Exception propagates | The caller decides whether to retry or report storage failure; stale data is not silently presented as current. |

`meta.json` is the commit marker, but it is not sufficient by itself. Every
Parquet footer must carry the same non-empty `generation_id` as the sidecar,
and that id must begin with `meta.generated_at + "-"`. Footer identity is read
from Parquet file metadata, so an empty but correctly stamped file remains a
valid generation.

The freshness threshold is:

```text
effective_cadence = max_cycle_seconds + meta.poll_interval_seconds
age = consumer_now - meta.generated_at
```

The sidecar is stale at `age >= effective_cadence`. A complete but stale
publication is still internally consistent; it is rejected only for current
use when freshness checking is requested. The loader does not return a status
object, so callers should track whether the returned value is a newly selected
fresh publication, a retained last-known publication, or `None`.

## Pairing and validation helpers

These public helpers are useful when a caller obtains objects through another
transport or wants to make the selection step explicit:

- `generation_ids(publication)` returns the sidecar, workflows-footer, and
  runs-footer ids without decoding rows. A missing or unreadable id is
  returned as `None`.
- `is_complete_generation(publication)` validates `meta`, requires all three
  ids to be equal and non-empty, and checks the id's timestamp prefix.
- `select_generation(candidate, last_complete, *, max_cycle_seconds=None,
  now=None)` returns the candidate only if it passes completeness and optional
  freshness checks; otherwise it returns `last_complete` unchanged.
- `freshness_age_seconds(meta, now)`,
  `freshness_threshold_seconds(meta, max_cycle_seconds)`, and
  `is_fresh(meta, now, max_cycle_seconds)` expose the heartbeat calculation.

`last_complete` is an already accepted value, not an arbitrary fallback. The
selection helpers preserve it by identity and never merge it with a rejected
candidate.

## Schema normalization and row views

The row helpers normalize stored files to the current schemas on every read
through `parquet_io.parquet_bytes_to_table`:

```python
current = current_snapshot_rows(selected)
history = historical_run_rows(selected)
```

- `current_snapshot_rows(publication)` reads `workflows.parquet` using
  `WORKFLOWS_SCHEMA`. It is for current inventory only.
- `historical_run_rows(publication)` reads `runs.parquet` using
  `RUNS_SCHEMA`. It is the source for history, rates, trends, durations, and
  failure counts.

Normalization casts existing columns to the declared type, inserts newly
introduced columns as typed nulls, and drops retired columns. This makes files
written by older releases readable without changing the generation-pairing
rule. A caller may normalize an individual object while diagnosing a torn
publication, but must not combine its rows with rows from another generation.

The historical convenience functions all use `runs.parquet` and never fall
back to the live snapshot:

- `historical_phase_counts(publication)`
- `historical_rates(publication)`
- `historical_trends(publication, bucket="day")`
- `historical_duration_history(publication)`
- `historical_failure_counts(publication, field="failure_class")`
- `historical_metrics(publication)`

The short aliases `snapshot_rows`, `run_rows`, `rates`, `trends`,
`duration_history`, and `failure_counts` are convenience aliases; the
source-explicit names above are the canonical API.

## Cluster availability

`cluster_availability(publication)` returns a mapping of cluster name to the
sidecar's strict boolean `ok` value:

```python
availability = cluster_availability(selected)
# {"ci": True, "staging": False}
```

`ok == false` means the exporter did not obtain a complete current listing for
that cluster. It may be unreachable, may have failed during pagination, or
may have returned malformed data. It does **not** mean the cluster is empty.
The sidecar reports `workflows: 0` for that cluster because zero rows were
included in this publication, not because zero workflows exist.

`current_snapshot_rows` filters the snapshot to rows from clusters with
`ok == true`. It does not copy the unavailable cluster's previous snapshot
forward. A failed cluster's retained rows in `runs.parquet` remain valid
historical ledger data, but they must not be used as a substitute for the
missing current inventory. A caller should expose the false availability flag
and preserve the distinction between “empty” and “unavailable”.

For the complete sidecar field and object contract, see
[`output-schema.md`](output-schema.md). For producer write ordering and the
exact torn-publication states, see
[`atomic-publication.md`](atomic-publication.md).
