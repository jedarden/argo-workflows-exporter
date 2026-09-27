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
| `trigger_kind` | string | nothing recorded | `cron`, `event`, or `user`; see [Trigger provenance and precedence](#trigger-provenance-and-precedence) |
| `trigger_name` | string | nothing recorded | the name selected by the trigger precedence rule; see [Trigger provenance and precedence](#trigger-provenance-and-precedence) |
| `phase` | string | never | `Pending`, `Running`, `Succeeded`, `Failed`, `Error`; an empty or absent source phase is normalized to `Pending` |
| `message` | string | usually, on success | Argo's own summary of why a run ended as it did |
| `progress` | string | not yet admitted | Argo's `N/M` completed-node counter, verbatim |
| `created_at` | string | never | RFC 3339, UTC |
| `started_at` | string | not yet admitted or never started | RFC 3339, UTC |
| `finished_at` | string | still running | RFC 3339, UTC |
| `duration_seconds` | int64 | not yet started or still running | wall clock, `finished_at - started_at` when both timestamps exist |
| `resources_duration_cpu` | int64 | not yet accumulated | see below |
| `resources_duration_memory` | int64 | not yet accumulated | see below |
| `failed_step` | string | not failed; nodes compressed | display name of the earliest failing **pod** node |
| `failed_step_message` | string | as above | that node's own message, which is usually more specific than `message` |
| `failure_fingerprint` | string | no failure message | 12 hex chars — see [Failure taxonomy](#failure-taxonomy) |
| `failure_class` | string | no failure message | `timeout`, `oom`, `clone_auth`, `image_pull`, `test_failure`, `lint`, `build`, `infrastructure`, or `unknown` |

`workflows.parquet` adds `observed_at` — the timestamp of the cycle that saw
it. `runs.parquet` adds `first_seen_at` and `last_seen_at` instead.

## Schema evolution

The two Parquet schemas are versioned by the exporter release, but a stored
object is not migrated in place. `src/parquet_io.py` is the compatibility
boundary: every object read for reuse or combination is conformed to the
current target schema by column name before its rows are interpreted. That
operation:

- fills a newly added nullable column with typed nulls when an older file does
  not have it;
- drops columns no longer in the current contract; and
- restores the current column order and declared types.

Consumers concatenating or diffing generations must apply the same
normalization to each generation before combining them. They must not ask
Parquet to concatenate the raw schemas from different exporter releases. The
contract is additive in place: new fields are nullable, and an existing
field's name and type do not change without a separately versioned migration.
For example, a `workflows.parquet` snapshot written before the failure
taxonomy has null `failure_fingerprint` and `failure_class` after
normalization, while a newer snapshot keeps its derived values; both then
share `WORKFLOWS_SCHEMA` and can be compared or concatenated safely. The
same rule applies to `runs.parquet`.

Schema conformance is independent of generation pairing. `generation_id` is
file metadata, not a row or schema column. Parquet objects written before
generation identity existed have no `generation_id` footer key; a footer read
returns no id, and the consumer must reject that object set as an incomplete
publication and retain the last complete generation. A missing key is not a
wildcard and must not be replaced with a guessed id. This differs from a
valid zero-row current snapshot, whose footer still carries its generation id.

On the producer side, a footerless `runs.parquet` is valid historical input:
the exporter reads and conforms its rows, then merges current observations.
The old footer value is never reused (and there may be no value at all). The
next successful cycle rewrites all three objects with one newly generated,
matching id. A read or compute failure writes none of them, preserving the
existing publication; a failed upload leaves the old sidecar as the commit
marker until a later retry completes the new set.

## Reading the columns

**`template` is null rather than guessed.** A workflow with a fully inline
`spec.templates` has no parent template. The name prefix is *not* used as a
fallback: `generateName` is free text, and a wrong grouping is worse than an
absent one. Group by `COALESCE(template, name)` if a bucket for inline runs
is wanted.

### Trigger provenance and precedence

`trigger_kind` and `trigger_name` are derived from Workflow labels in this
fixed order, highest precedence first:

1. A non-empty `workflows.argoproj.io/cron-workflow` label produces
   `trigger_kind = "cron"` and `trigger_name` equal to that label's cron
   workflow name.
2. Otherwise, a non-empty `events.argoproj.io/sensor` label produces
   `trigger_kind = "event"`. If the Workflow also has a non-empty
   `events.argoproj.io/trigger` label, `trigger_name` is that specific Argo
   Events trigger name. Otherwise, `trigger_name` falls back to the sensor
   name.
3. Otherwise, a non-empty `workflows.argoproj.io/creator` label produces
   `trigger_kind = "user"` and `trigger_name` equal to the creator value.
4. If none of those labels is present, both columns are null.

This means cron wins over event and user labels, and event wins over a creator
label. The event trigger name is preferred because a sensor can contain
multiple triggers. The sensor name is not stored in another output column and
is discarded when a trigger label is available. A trigger label without its
sensor label does not establish event provenance; the extractor continues to
the creator fallback.

**`duration_seconds` is null while running**, deliberately — not "elapsed so
far". Age of a live run is `observed_at - started_at`, computed by the
consumer; putting it in the same column as a final duration would make
running and finished runs indistinguishable.

**Admission and never-started normalization.** Argo can expose a Workflow
before the controller admits it, with an empty `status.phase` and no
`startedAt`. The exporter emits `phase = "Pending"`, `started_at = null`, and
`duration_seconds = null`; this keeps the exported phase non-null while
retaining the distinction between an Argo source value and the output value.
An `Error` workflow can be terminal without ever starting and can still carry
`finishedAt`. For that shape the exporter emits `phase = "Error"`, preserves
`finished_at`, emits `started_at = null`, and leaves `duration_seconds = null`:
duration is only defined when both endpoints of the interval are present.

**`resources_duration_*` are Argo's own accumulated counters** (`cpu` and
`memory` from `status.resourcesDuration`). They are useful as relative cost
indicators between runs of the same pipeline; do not present them as
absolute CPU-seconds or bytes without verifying the units against the Argo
version in use. Extended-resource keys such as GPUs are dropped; only `cpu`
and `memory` are mapped to output columns.

**`failed_step` is a convenience, not a guarantee.** Argo compresses the node
tree into `status.compressedNodes` on very large workflows. The exporter
decodes that base64+gzip node map when `status.nodes` is absent; malformed
compressed data is ignored, leaving `failed_step` null while still carrying
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
| `version` | string | non-empty | the exporter release that wrote this generation; `"unknown"` if the VERSION file is missing, unreadable, or empty |
| `generated_at` | string | RFC 3339 UTC, second resolution, literal `Z` (`%Y-%m-%dT%H:%M:%SZ`) | the cycle-start instant captured before collection; see the heartbeat note below |
| `generation_id` | string | `<generated_at>` + `-` + 12 lowercase hex | this publication's identity — see below |
| `poll_interval_seconds` | integer | ≥ 1 | the exporter's post-cycle delay; combine it with the consumer's `C_max` to judge `generated_at` freshness |
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

`generated_at` is captured once, at the beginning of a cycle before any
Kubernetes or S3 I/O. The same value is passed to row extraction and ledger
merging, used as the `generation_id` timestamp prefix, and written to
`meta.json` if publication commits. It is not recomputed at successful
computation or upload completion. Thus a slow cycle's age includes its
runtime, while a failed publication leaves the previous committed timestamp
unchanged.

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

`generated_at` doubles as the collection heartbeat: although its value is
captured at cycle start, it is only committed after at least one cluster
completes its listing and all three objects are published. A consumer can
therefore detect a stalled or failing exporter by its age. `clusters[].ok`
distinguishes a partial outage — everything else was still collected and
written.

### Heartbeat freshness

The consumer must supply `C_max`, its configured upper bound for one complete
exporter cycle. The effective cadence is:

```text
effective_cadence = C_max + meta.poll_interval_seconds
age = consumer_now - meta.generated_at
```

The subtraction uses the committed cycle-start instant in `meta.generated_at`.
It does not use the consumer's download time, the Parquet footer timestamp, or
an upload completion time. The `generation_id` prefix is required to be the
same instant, so generation pairing and freshness cannot describe different
moments.

The sidecar is **fresh** only while `age < effective_cadence`; it is **stale**
at `age >= effective_cadence`. This is the same serial-cycle bound used for
the observation-window calculation in
[`ttl-and-observation-windows.md`](ttl-and-observation-windows.md): the poll
interval is the quiet time after a cycle, and `C_max` accounts for the next
cycle's runtime. The threshold is therefore not a hardcoded number and must
not be replaced by the exporter health endpoint's separate probe policy.

A cycle in which every cluster fails the listing, or whose publication fails,
writes no new commit marker. Repeated failed cycles therefore leave all three
objects internally consistent or detectably torn while freezing the committed
`generated_at`. Generation pairing alone therefore does not establish
currentness. When the sidecar is stale, the consumer must emit an operational
alert containing the generation id, age, and threshold; hold the last complete
paired generation as **last known data**; and mark it unavailable for current
use. It must never present that held generation as current. If no complete
generation has been held yet, current data is unavailable rather than an empty
snapshot.

`generation_id` identifies the publication as a whole: the same id is
embedded in both Parquet files' file-level metadata, and all three objects of
one cycle always carry the same id. It is what makes a torn publication —
one of the three writes failing partway — detectable instead of silently
misread. The embedded timestamp is the cycle's `generated_at`, so ids sort
with the cycle-start time. A consumer must reject an id whose timestamp prefix
disagrees with `meta.generated_at`. See [Atomic publication](atomic-publication.md)
for the write ordering, retry behavior, and the exact state each failure
leaves behind.

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

Malformed answers are unavailable too. Invalid JSON, a list response with a
missing or non-array `items` field, a non-string or repeated continuation
token, and a malformed individual `Workflow` object are logged as errors and
mark that cluster `ok: false`. Rows already received from that cluster are
discarded; a malformed item never becomes a blank row and never leaves a
partial cluster snapshot behind. The other clusters are still collected and
may be published. If every cluster is unavailable, publication is skipped and
the previous generation remains untouched.

If every cluster is unavailable, none of the three objects is written. The
previous objects remain as the last published generation and
`meta.generated_at` stops advancing; if there was no previous generation,
`meta.json` remains absent. `meta.json` does not preserve a separate last
successful cluster snapshot, so consumers that need one must archive earlier
outputs themselves.

### Before the first publication

A consumer can be pointed at `DEST_S3_PREFIX` before the exporter has
completed its first successful cycle. In that bootstrap state, `meta.json`
may be absent, with or without leftover data objects from an interrupted
upload. A missing `meta.json` is **not an error** and is not a valid empty
generation: the consumer must report **no generation published yet**, return
no current rows, and must not read or interpret any Parquet object that has no
commit marker. Reading the marker first also means an entirely empty prefix
and a prefix containing only an uncommitted data object have the same safe
result.

If `meta.json` is present but either Parquet object is absent, the candidate is
also incomplete. The consumer must retain the last complete generation when
one exists; before the first complete generation it must again report no
generation published yet, without treating the missing object as an error.
Only a sidecar and both Parquet files carrying the same non-empty
`generation_id` establish a generation. A paired zero-row snapshot remains a
valid published generation.

### Consumer contract

1. Read `meta.json` before interpreting either Parquet file.
2. Check `generated_at` using the heartbeat freshness contract above. During
   bootstrap, missing `meta.json` means no generation has been published yet,
   as described above; after a prior publication, missing or stale metadata
   means current data is unavailable for every cluster, so emit an alert and
   hold the prior complete generation only as last-known data, never as
   current data.
3. **Check the generation pairing before mixing objects.** Compare
   `meta.json`'s `generation_id` with the `generation_id` in each Parquet
   file's file-level metadata (the Parquet footer alone —
   `pyarrow.parquet.read_schema`, not a full object read). All three must be
   equal. A mismatch or a missing footer key means one object is legacy,
   missing, or was uploaded from a different cycle: hold the previously paired
   generation and retry later rather than presenting an incomplete or mixed
   set. The check works for empty snapshots too — a valid id is in the file
   metadata, not the rows.
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
