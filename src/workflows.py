"""Turns raw Argo `Workflow` objects into flat rows.

Everything here reads only fields upstream Argo itself sets, so the same
extraction works against any Argo Workflows installation.
"""

import logging
from datetime import datetime

from .k8s_api import list_items, list_path

log = logging.getLogger(__name__)

# Labels Argo (and Argo Events) set on the Workflow objects they create.
LABEL_TEMPLATE = "workflows.argoproj.io/workflow-template"
LABEL_CLUSTER_TEMPLATE = "workflows.argoproj.io/cluster-workflow-template"
LABEL_CRON = "workflows.argoproj.io/cron-workflow"
LABEL_CREATOR = "workflows.argoproj.io/creator"
LABEL_SENSOR = "events.argoproj.io/sensor"
LABEL_TRIGGER = "events.argoproj.io/trigger"

_TERMINAL_NODE_PHASES = ("Failed", "Error")


def _parse_ts(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        log.debug("unparseable timestamp: %r", value)
        return None


def duration_seconds(started_at, finished_at):
    """Wall-clock seconds from start to finish, or None while a run is still
    going (or never started). Deliberately not "age so far" — a consumer can
    compute that from `started_at` and the snapshot's own timestamp, and
    conflating the two would make a running workflow indistinguishable from a
    finished one in the same column."""
    start, finish = _parse_ts(started_at), _parse_ts(finished_at)
    if start is None or finish is None:
        return None
    return int((finish - start).total_seconds())


def template_of(wf):
    """The WorkflowTemplate a run came from, as `(name, scope)`.

    Three sources, in descending reliability:

    1. `spec.workflowTemplateRef` — present on anything submitted against a
       template, and the only one that survives when Argo's label-injection
       is off.
    2. The template labels — set by some submission paths and not others.
    3. Nothing: workflows with a fully inline `spec.templates` genuinely have
       no parent template, and get (None, None) rather than a guess. Do not
       be tempted to infer one from the name prefix; `generateName` is free
       text and a wrong grouping is worse than an absent one.
    """
    ref = (wf.get("spec") or {}).get("workflowTemplateRef") or {}
    if ref.get("name"):
        return ref["name"], "cluster" if ref.get("clusterScope") else "namespaced"

    labels = (wf.get("metadata") or {}).get("labels") or {}
    if labels.get(LABEL_CLUSTER_TEMPLATE):
        return labels[LABEL_CLUSTER_TEMPLATE], "cluster"
    if labels.get(LABEL_TEMPLATE):
        return labels[LABEL_TEMPLATE], "namespaced"
    return None, None


def trigger_of(wf):
    """What caused this run, as `(kind, name)` — one of `cron`, `event`,
    `user`, or (None, None) when nothing identifying was recorded."""
    labels = (wf.get("metadata") or {}).get("labels") or {}

    if labels.get(LABEL_CRON):
        return "cron", labels[LABEL_CRON]

    # An Argo Events sensor names both itself and the specific trigger within
    # it; the trigger is the more useful of the two because one sensor
    # commonly fans out to many.
    if labels.get(LABEL_SENSOR):
        return "event", labels.get(LABEL_TRIGGER) or labels[LABEL_SENSOR]

    if labels.get(LABEL_CREATOR):
        return "user", labels[LABEL_CREATOR]
    return None, None


def failed_step(wf):
    """`(display_name, message)` of the step that failed, or (None, None).

    Only pod nodes are considered: when a step fails, its parent DAG/steps
    nodes fail too, and reporting those would name the whole workflow back to
    itself instead of the thing that broke. Earliest failure wins — later
    ones are usually consequences of it.

    Returns (None, None) when Argo has compressed the node tree into
    `status.compressedNodes` (it does this for very large workflows). That is
    a deliberate omission rather than a decompression step: the field is a
    convenience, and `message` below still carries Argo's own summary.
    """
    nodes = (wf.get("status") or {}).get("nodes") or {}
    failures = [
        n for n in nodes.values()
        if n.get("phase") in _TERMINAL_NODE_PHASES and n.get("type") == "Pod"
    ]
    if not failures:
        return None, None
    earliest = min(failures, key=lambda n: n.get("startedAt") or "")
    return earliest.get("displayName") or earliest.get("name"), earliest.get("message")


def to_row(wf, cluster_name: str, observed_at: str) -> dict:
    meta = wf.get("metadata") or {}
    status = wf.get("status") or {}
    template, template_scope = template_of(wf)
    trigger_kind, trigger_name = trigger_of(wf)
    step_name, step_message = failed_step(wf)
    resources = status.get("resourcesDuration") or {}

    return {
        "uid": meta.get("uid", ""),
        "cluster": cluster_name,
        "namespace": meta.get("namespace", ""),
        "name": meta.get("name", ""),
        "template": template,
        "template_scope": template_scope,
        "trigger_kind": trigger_kind,
        "trigger_name": trigger_name,
        # `status.phase` is empty on a workflow the controller has not
        # admitted yet. "Pending" is what the Argo UI shows for that state.
        "phase": status.get("phase") or "Pending",
        "message": status.get("message"),
        "progress": status.get("progress"),
        "created_at": meta.get("creationTimestamp"),
        "started_at": status.get("startedAt"),
        "finished_at": status.get("finishedAt"),
        "duration_seconds": duration_seconds(status.get("startedAt"), status.get("finishedAt")),
        # Argo's own accumulated resource counters. Only cpu and memory are
        # kept as columns; an installation using extended resources (GPUs,
        # ephemeral-storage) will find those keys dropped.
        "resources_duration_cpu": resources.get("cpu"),
        "resources_duration_memory": resources.get("memory"),
        "failed_step": step_name,
        "failed_step_message": step_message,
        "observed_at": observed_at,
    }


def fetch_workflows(clusters, default_namespace: str, timeout: int, page_size: int, observed_at: str):
    """Returns `(rows, cluster_stats)` — one row per Workflow object that
    currently exists, across every reachable cluster.

    A cluster that fails or answers only partially contributes no rows and is
    reported `ok: false`, rather than contributing what did arrive. Consumers
    read a missing workflow as a deleted one, so a half-answer is worse than
    no answer: it would show runs vanishing that are still there.
    """
    rows, stats = [], []
    for cluster in clusters:
        namespace = cluster.namespace if cluster.namespace is not None else default_namespace
        items, complete = list_items(
            cluster, list_path(namespace, "workflows"), timeout, page_size
        )
        if not complete:
            log.warning(
                "%s: incomplete workflow listing (%d item(s) before the failure), "
                "skipping this cluster for this cycle", cluster.name, len(items)
            )
            stats.append({"name": cluster.name, "ok": False, "workflows": 0})
            continue

        cluster_rows = [to_row(wf, cluster.name, observed_at) for wf in items]
        rows.extend(cluster_rows)
        stats.append({"name": cluster.name, "ok": True, "workflows": len(cluster_rows)})
        log.info("%s: %d workflow(s)", cluster.name, len(cluster_rows))
    return rows, stats
