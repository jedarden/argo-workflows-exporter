# Argo `Workflow` object — fields relevant to the exporter contract

Reference notes on the upstream object shape, gathered from the Argo
Workflows API and confirmed against a live installation (Argo Workflows v3.x,
64 objects across templated, event-triggered, cron-triggered and inline
workflows). The status table includes both fields the exporter reads and
upstream fields retained here for reference but intentionally excluded from
the output contract.

## Identity and provenance

| Path | Notes |
|---|---|
| `metadata.uid` | present on every object; the only stable key across cycles |
| `metadata.name` | generated, e.g. `<template>-<suffix>`; not unique over time |
| `metadata.creationTimestamp` | RFC 3339, second precision |
| `spec.workflowTemplateRef.name` | present whenever a run was submitted against a template |
| `spec.workflowTemplateRef.clusterScope` | absent/false for namespaced templates |
| `spec.templates` | present instead, on fully inline workflows |

### Labels

Set by Argo and Argo Events. Observed frequencies in the sample, out of 64:

| Label | Count | Meaning |
|---|---|---|
| `workflows.argoproj.io/phase` | 64 | mirrors `status.phase`; `status` is authoritative |
| `workflows.argoproj.io/completed` | 64 | `"true"` / `"false"` |
| `workflows.argoproj.io/creator` | 48 | submitting user or ServiceAccount |
| `events.argoproj.io/sensor` | 48 | Argo Events sensor that fired |
| `events.argoproj.io/trigger` | 48 | the specific trigger within that sensor |
| `workflows.argoproj.io/workflow-template` | 0 | — |

**The template label is not reliable.** It was absent on every object in the
sample, including the 56 that carried a `spec.workflowTemplateRef`. Grouping
by label alone would have found nothing. `spec.workflowTemplateRef` is the
primary source; the label is only a fallback for submission paths that set it
and omit the ref.

Cron-triggered runs carry `workflows.argoproj.io/cron-workflow`.
`metadata.ownerReferences` was empty on every object in the sample, so it is
not a usable path to the parent.

## Status

| Path | Notes |
|---|---|
| `status.phase` | `Pending`, `Running`, `Succeeded`, `Failed`, `Error`. **Empty** on an object the controller has not admitted yet — the UI shows that state as Pending. The exporter normalizes an empty or absent source value to output `phase = "Pending"`, so its phase column remains non-null and limited to those five values. |
| `status.message` | failure summary, e.g. `workflowtemplates.argoproj.io "x" not found` |
| `status.progress` | `"N/M"` completed nodes |
| `status.startedAt` / `finishedAt` | RFC 3339; `finishedAt` is absent while running, but a never-started terminal `Error` can have `finishedAt` without `startedAt` |
| `status.resourcesDuration` | `{"cpu": int, "memory": int}`, accumulated over the run; other resource keys appear for extended resources |
| `status.estimatedDuration` | **Reference-only; intentionally ignored.** Absent on all 64 objects — populated only when Argo has a comparable prior run; the exporter does not read or emit it |
| `status.nodes` | map of node name to node; up to 8 entries in the sample |
| `status.compressedNodes` | replaces `status.nodes` on very large workflows (gzip + base64 of the node map) |
| `status.conditions` | **Intentionally ignored.** Condition records such as `PodRunning` and `Completed` are not read or emitted; no output column represents them |
| `status.storedTemplates` | **Intentionally ignored.** The full resolved template body is inlined into the object and can be its largest contributor to size; the exporter does not collect or emit it |

### Nodes

Each entry in `status.nodes` carries `type` (`Pod`, `Steps`, `DAG`,
`Retry`, ...), `phase`, `displayName`, `startedAt` and `message`. When a step
fails, its ancestor `Steps`/`DAG` nodes fail with it, so identifying "the step
that failed" means filtering to `type == "Pod"` and taking the earliest valid,
timezone-aware `startedAt`. The exporter puts missing or invalid timestamps
after timestamped nodes and uses the node-map key lexicographically for both
that fallback group and ties.

An observed example of why the node message is worth keeping separately: a
workflow whose `status.message` said only that a child failed, where the pod
node's message read `The node was low on resource: memory` — a materially
different diagnosis.

## The `Error` phase

Distinct from `Failed`: `Error` covers runs that never executed, such as a
submission naming a template that does not exist. Eight of the 64 sampled
objects were in this state, with `progress: "0/0"` and no
`resourcesDuration`. They are worth separating from `Failed` in any success
metric — nothing ran, so they say nothing about the pipeline's health, only
about its wiring.

For the exporter, an `Error` object with `finishedAt` but no `startedAt` retains
that finish timestamp, emits `started_at = null`, and emits
`duration_seconds = null`: there is no start endpoint from which to calculate a
duration. This is the same null-duration rule used for an un-admitted object,
whose empty source phase is exported as `Pending`.

## Listing and pagination

The Kubernetes API list endpoints are:

```
/apis/argoproj.io/v1alpha1/workflows                        # all namespaces
/apis/argoproj.io/v1alpha1/namespaces/<ns>/workflows        # one namespace
```

Both accept `?limit=N` and return `metadata.continue` when more pages remain;
the token is passed back as `?continue=<token>`. A `continue` token expires
(typically ~5 minutes), after which the API returns 410 Gone and the listing
must restart.

Note that field selectors do **not** support inequality on timestamps, so
there is no server-side way to ask for "workflows created since T" — filtering
by time is always client-side.
