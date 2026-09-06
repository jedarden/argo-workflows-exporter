# Output schema

Three objects under `DEST_S3_PREFIX`. Both Parquet files share a common set
of columns and differ only in their timestamp columns.

## Shared columns

| Column | Type | Null when | Notes |
|---|---|---|---|
| `uid` | string | never | `metadata.uid`. The stable identity of a run — names are not unique over time, UIDs are. |
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

One row per run ever observed, keyed by `uid`.

- A run seen again keeps its original `first_seen_at` and takes every other
  value from the newest observation.
- A run **not** seen this cycle is left exactly as it was. This is the normal
  end state: Argo deleted the object, and the last observation is the record.
- Rows are dropped `RUN_RETENTION_DAYS` after `last_seen_at`. Measuring from
  last observation rather than from `started_at` means a long-running
  workflow is never expired while it is still alive.

`first_seen_at` and `last_seen_at` describe *this exporter's* view, not the
run. A run that finished long before the exporter first started will show a
`first_seen_at` well after its own `finished_at`.

## `meta.json`

```json
{
  "version": "0.2.0",
  "generated_at": "2026-08-11T04:00:00Z",
  "poll_interval_seconds": 300,
  "run_retention_days": 7,
  "clusters": [{"name": "ci", "ok": true, "workflows": 64}],
  "workflows": 64,
  "runs": 812
}
```

`generated_at` doubles as the collection heartbeat: it is only written on a
cycle that reached at least one cluster, so a consumer can detect a stalled
or failing exporter by its age alone. `clusters[].ok` distinguishes a partial
outage — everything else was still collected and written.
