# Output schema

Three objects under `DEST_S3_PREFIX`. Both Parquet files share a common set
of columns and differ only in their timestamp columns.

## Shared columns

| Column | Type | Null when | Notes |
|---|---|---|---|
| `uid` | string | never | `metadata.uid`. Unique within its cluster — names are not unique over time. A run's ledger identity is (`cluster`, `uid`); see [`runs.parquet` semantics](#runsparquet-semantics). |
| `cluster` | string | never | the `name` given in `CLUSTERS_JSON` |
| `namespace` | string | never | |
| `name` | string | never | the generated object name |
| `template` | string | inline workflows | the `WorkflowTemplate` a run came from |
| `template_scope` | string | inline workflows | `namespaced` or `cluster` |
| `trigger_kind` | string | nothing recorded | `cron`, `event`, or `user` |
| `trigger_name` | string | nothing recorded | cron workflow name, Argo Events trigger name, or creator |
| `phase` | string | never | `Pending`, `Running`, `Succeeded`, `Failed`, `Error` |
| `message` | string | usually, on success | Argo's own summary of why a run ended as it did |
| `progress` | string | not yet admitted | Argo's `N/M` completed-node counter, verbatim |
| `created_at` | string | never | RFC 3339, UTC |
| `started_at` | string | not yet admitted | RFC 3339, UTC |
| `finished_at` | string | still running | RFC 3339, UTC |
| `duration_seconds` | int64 | still running | wall clock, `finished_at - started_at` |
| `resources_duration_cpu` | int64 | not yet accumulated | see below |
| `resources_duration_memory` | int64 | not yet accumulated | see below |
| `failed_step` | string | not failed; nodes compressed | display name of the earliest failing **pod** node |
| `failed_step_message` | string | as above | that node's own message, which is usually more specific than `message` |
| `failure_fingerprint` | string | no failure message | 12 hex chars — see [Failure taxonomy](#failure-taxonomy) |
| `failure_class` | string | no failure message | `timeout`, `oom`, `clone_auth`, `image_pull`, `test_failure`, `lint`, `build`, `infrastructure`, or `unknown` |

`workflows.parquet` adds `observed_at` — the timestamp of the cycle that saw
it. `runs.parquet` adds `first_seen_at` and `last_seen_at` instead.

## Reading the columns

**`template` is null rather than guessed.** A workflow with a fully inline
`spec.templates` has no parent template. The name prefix is *not* used as a
fallback: `generateName` is free text, and a wrong grouping is worse than an
absent one. Group by `COALESCE(template, name)` if a bucket for inline runs
is wanted.

**`duration_seconds` is null while running**, deliberately — not "elapsed so
far". Age of a live run is `observed_at - started_at`, computed by the
consumer; putting it in the same column as a final duration would make
running and finished runs indistinguishable.

**`resources_duration_*` are Argo's own accumulated counters** (`cpu` and
`memory` from `status.resourcesDuration`). They are useful as relative cost
indicators between runs of the same pipeline; do not present them as
absolute CPU-seconds or bytes without verifying the units against the Argo
version in use. Extended resources such as GPUs are not kept as columns.

**`failed_step` is a convenience, not a guarantee.** Argo compresses the node
tree into `status.compressedNodes` on very large workflows, and this exporter
does not decompress it — those rows get a null `failed_step` and still carry
`message`.

## Failure taxonomy

`failure_fingerprint` and `failure_class` are derived, and their raw input is
kept: both are computed from `failed_step_message`, falling back to `message`
when the failing step is unknown (compressed nodes, or the failure is at
workflow level). A run with neither has a failure to group but nothing to say
about it, and both columns stay null — they are never empty strings.

**`failure_fingerprint`** is the first 12 hex characters of the SHA-256 of the
message after normalization: lowercased, with every instance-specific token
replaced by a placeholder — UUIDs, timestamps, URLs, absolute paths, durations
(`1h2m3s`), generated pod and node names, IP addresses, hex hashes and numbers
over three digits — and whitespace collapsed. Two runs that failed for the
same reason differ in exactly those tokens, so they collapse to one
fingerprint and can be counted as one failure; a fingerprint shared by
hundreds of runs is a real recurring problem, and one seen twice is not two.

It is unsalted and unversioned on purpose. Its value is joining failures
observed at different times (a retry against its original, this week's runs
against last month's), so a re-derivation that produced new values would
silently break every join already in flight. The normalization rules in
`src/workflows.py` are therefore append-mostly: tightening a rule changes
future fingerprints, and existing stored values are never recomputed.

**`failure_class`** comes from `src/failure_classes.yaml`, an ordered table of
regexes — first match wins, so the more specific class sits above the more
general one. Classes are decided against the *raw* message, not the
normalized text, because normalization erases exactly the tokens (exit codes,
durations, image tags) that tell a build failure from a test one. `unknown` is
the fallback when nothing matches and takes no rules of its own.

The table ships with the image and is not operator-configurable: it is
reviewed content, and a rule that misfires is a bug to fix here, not a knob.
When a rule is added, add the message that justifies it to
`tests/test_workflows.py::FAILURE_MESSAGES` — every class must stay reachable
by a real message, and a rule with no witness is how a taxonomy silently rots.

Both columns are additive. A ledger written before 0.2.0 reads back with them
null, and a run re-observed after the upgrade gains them on its next
observation; a run Argo has already reaped keeps its nulls, because its raw
message is preserved but is never reprocessed.

## `runs.parquet` semantics

One row per run ever observed, keyed by (`cluster`, `uid`).

- A run seen again keeps its original `first_seen_at` and takes every other
  value from the newest observation.
- A run **not** seen this cycle is left exactly as it was. This is the normal
  end state: Argo deleted the object, and the last observation is the record.
- Rows are dropped `RUN_RETENTION_DAYS` after `last_seen_at`. Measuring from
  last observation rather than from `started_at` means a long-running
  workflow is never expired while it is still alive.

**Why the key is the pair, not `uid` alone.** Kubernetes scopes a UID's
uniqueness to a single cluster's etcd and no further — nothing stops two of
the configured clusters from minting the same one. A uid-only key would let
the second cluster's observation overwrite the first's row on every cycle,
silently keeping one run's history for what were two. Cluster names are
validated unique in `CLUSTERS_JSON`, which makes the pair unambiguous.

**Migration.** The key change is invisible on disk: `cluster` has been part
of every row since the first release (both files share one schema), so a
ledger written by any earlier version reads back unchanged and every run
still matches its own row. The difference shows only where the old key was
lossy. Two runs from different clusters that shared a uid were collapsing
into one row, each cycle overwriting the other under whichever cluster
wrote first; after the upgrade, the surviving row stays with the cluster
that last wrote it, and the other cluster's run enters as a new row with a
fresh `first_seen_at` on its next observation. That collapsed pre-upgrade
history is already merged and is not un-merged. Renaming a cluster in
`CLUSTERS_JSON` has the same shape by the same rule: rows under the old
name are kept until retention expires them, and the renamed cluster's runs
start new identities.

`first_seen_at` and `last_seen_at` describe *this exporter's* view, not the
run. A run that finished long before the exporter first started will show a
`first_seen_at` well after its own `finished_at`.

## `meta.json`

```json
{
  "version": "0.2.0",
  "generated_at": "2026-08-11T04:00:00Z",
  "generation_id": "2026-08-11T04:00:00Z-3f9c2a1b7d44",
  "poll_interval_seconds": 300,
  "run_retention_days": 7,
  "clusters": [{"name": "ci", "ok": true, "workflows": 64}],
  "workflows": 64,
  "runs": 812
}
```

### Field contract

The sidecar has exactly the eight top-level fields above — no others — with
these names and types. The contract is enforced by the exporter itself:
`src/meta_schema.py` holds it as a JSON Schema document (`META_SCHEMA`) plus
the cross-field rules, and every cycle validates its sidecar against it
before uploading. A violation refuses the publication rather than shipping a
commit marker that misdescribes the generation beside it. Consumers may
apply the same schema to downloaded sidecars.

| Field | Type | Constraint | Meaning |
|---|---|---|---|
| `version` | string | non-empty | the exporter release that wrote this generation; `"unknown"` if built without a VERSION file |
| `generated_at` | string | RFC 3339 UTC, second resolution, literal `Z` (`%Y-%m-%dT%H:%M:%SZ`) | when this cycle ran — see the heartbeat note below |
| `generation_id` | string | `<generated_at>` + `-` + 12 lowercase hex | this publication's identity — see below |
| `poll_interval_seconds` | integer | ≥ 1 | the exporter's configured poll interval; what `generated_at`'s freshness should be judged against |
| `run_retention_days` | integer | ≥ 1 | the configured `RUN_RETENTION_DAYS`; how long `runs.parquet` keeps unseen runs |
| `clusters` | array | ≥ 1 entry | exactly one entry per configured cluster, in `CLUSTERS_JSON` order |
| `clusters[].name` | string | non-empty | the cluster's `name` from `CLUSTERS_JSON` |
| `clusters[].ok` | boolean | strict `true`/`false` | whether this cycle completed that cluster's full listing |
| `clusters[].workflows` | integer | ≥ 0 | rows this cycle's `workflows.parquet` carries from that cluster |
| `workflows` | integer | ≥ 0 | total rows in this cycle's `workflows.parquet`; equals the sum of `clusters[].workflows` over `ok == true` entries |
| `runs` | integer | ≥ 0 | total rows in this cycle's `runs.parquet` — the whole ledger after retention, not this cycle's observations |

Timestamps are always RFC 3339 UTC at second resolution with a literal `Z`
suffix — never an offset, never sub-second precision, never a naive local
time. This holds for `generated_at`, for the timestamp embedded in
`generation_id`, and for the Parquet timestamp columns.

**Row-count meanings.** The two counts are not parallel and must not be read
as if they were:

- `workflows` counts only what was published this cycle. A cluster with
  `ok == false` contributes `0` and no rows — that zero is "nothing
  included", **not** a claim that the cluster has no workflows (see
  [Snapshot availability](#snapshot-availability)). `clusters[].workflows`
  sums to it, always.
- `runs` counts the entire ledger, so it includes runs from a cluster that
  did not answer this cycle. A failed cluster removes its workflows rows
  from the new snapshot but keeps its runs: history is never withdrawn
  because a listing failed. That is why `runs` can exceed `workflows` even
  in a fully healthy cycle — runs persist after Argo deletes them, snapshot
  rows do not.

`generated_at` doubles as the collection heartbeat: it is only written after
at least one cluster completes its listing, so a consumer can detect a stalled
or failing exporter by its age alone. `clusters[].ok` distinguishes a partial
outage — everything else was still collected and written.

`generation_id` identifies the publication as a whole: the same id is
embedded in both Parquet files' file-level metadata, and all three objects of
one cycle always carry the same id. It is what makes a torn publication —
one of the three writes failing partway — detectable instead of silently
misread. The embedded timestamp is the cycle's `generated_at`, so ids sort
with time. See [Atomic publication](atomic-publication.md) for the write
ordering, retry behavior, and the exact state each failure leaves behind.

### Snapshot availability

`workflows.parquet` contains complete snapshots only for clusters whose listing
finished successfully:

- `clusters[].ok == true` and `workflows > 0` means that cluster completed its
  listing and this many of its rows are present.
- `clusters[].ok == true` and `workflows == 0` means that cluster completed its
  listing and was confirmed empty.
- `clusters[].ok == false` means the cluster was unreachable or any page of
  its listing failed. Its `workflows` value is always `0`, but that is the count
  of included rows, **not** a claim that the cluster has no workflows.

An unavailable cluster contributes no rows to the newly published
`workflows.parquet`, including items returned before pagination failed. Its
rows from the previous successful snapshot are not copied forward. If at least
one other cluster succeeds, the new file is a union of the successfully listed
clusters only, and `meta.json` is updated with the same per-cluster `ok` status.
Top-level `workflows` counts only those included rows.

If every cluster is unavailable, none of the three objects is written. The
previous objects remain as the last published generation and
`meta.generated_at` stops advancing; if there was no previous generation,
`meta.json` remains absent. `meta.json` does not preserve a separate last
successful cluster snapshot, so consumers that need one must archive earlier
outputs themselves.

### Consumer contract

1. Read `meta.json` before interpreting either Parquet file.
2. Check `generated_at` against the expected polling interval and the
   consumer's freshness policy. Missing or stale metadata means current data is
   unavailable for every cluster; the old Parquet is only a last-known snapshot.
3. **Check the generation pairing before mixing objects.** Compare
   `meta.json`'s `generation_id` with the `generation_id` in each Parquet
   file's file-level metadata (the Parquet footer alone —
   `pyarrow.parquet.read_schema`, not a full object read). All three must be
   equal. A mismatch means one upload failed partway and the stored objects
   belong to two different cycles: hold the previously paired generation and
   retry later rather than presenting two cycles as one. The check works for
   empty snapshots too — the id is in the file metadata, not the rows.
4. For fresh, paired metadata, use rows only for clusters with `ok == true`.
   Treat an `ok == false` cluster as unavailable, not empty: absence from that
   cluster does not indicate deletion. A non-empty snapshot's rows must also
   carry `observed_at == generated_at`, which follows from the pairing.
5. Use `runs.parquet` for historical run state, not as a substitute for an
   unavailable current cluster snapshot. Its identity is (`cluster`, `uid`) —
   join and deduplicate on the pair, never on `uid` alone.

The three objects are written in the order shown above — data objects first,
`meta.json` last as the commit marker — but they are not an atomic S3
transaction, and a failed upload can leave a new Parquet beside the old
sidecar. Step 3 is what turns that from a silent misreading into a detected
one; the write ordering, retry behavior, and the exact state each failure
leaves behind are specified in
[Atomic publication](atomic-publication.md).
