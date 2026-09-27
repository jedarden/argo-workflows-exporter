"""Turns raw Argo `Workflow` objects into flat rows.

Everything here reads only fields upstream Argo itself sets, so the same
extraction works against any Argo Workflows installation.
"""

import base64
import binascii
import gzip
import json
import logging
from datetime import datetime, timezone

from .failure_taxonomy import (
    _load_failure_classes,
    failure_class,
    failure_columns,
    normalize_failure,
)
from .k8s_api import KubernetesResponseError, list_items, list_path

log = logging.getLogger(__name__)

# Labels Argo (and Argo Events) set on the Workflow objects they create.
LABEL_TEMPLATE = "workflows.argoproj.io/workflow-template"
LABEL_CLUSTER_TEMPLATE = "workflows.argoproj.io/cluster-workflow-template"
LABEL_CRON = "workflows.argoproj.io/cron-workflow"
LABEL_CREATOR = "workflows.argoproj.io/creator"
LABEL_SENSOR = "events.argoproj.io/sensor"
LABEL_TRIGGER = "events.argoproj.io/trigger"

_TERMINAL_NODE_PHASES = ("Failed", "Error")


class MalformedWorkflowError(ValueError):
    """A list item cannot be represented as a Workflow snapshot row."""


def _workflow_location(index):
    return f"items[{index}]" if index is not None else "workflow"


def _require_workflow_mapping(value, location, field):
    if not isinstance(value, dict):
        raise MalformedWorkflowError(
            f"{location}: '{field}' must be a JSON object, got {type(value).__name__}"
        )


def _require_workflow_string(value, location, field, required=False):
    if value is None and not required:
        return
    if not isinstance(value, str) or (required and not value.strip()):
        requirement = "non-empty string" if required else "string or null"
        raise MalformedWorkflowError(
            f"{location}: '{field}' must be a {requirement}"
        )


def _validate_workflow(wf, index=None):
    """Reject malformed list items before they can become blank or partial rows.

    The Kubernetes API guarantees object-shaped Workflow resources, but the
    exporter still validates the fields it reads. In particular, uid/name/
    namespace are the snapshot identity, while the optional nested maps are
    required to have the shape the extractors expect. A cluster with one bad
    item is unavailable for this cycle; valid items from that same response
    must not be published as a misleading partial snapshot.
    """
    location = _workflow_location(index)
    if not isinstance(wf, dict):
        raise MalformedWorkflowError(
            f"{location}: expected a JSON object, got {type(wf).__name__}"
        )

    metadata = wf.get("metadata")
    _require_workflow_mapping(metadata, location, "metadata")
    for field in ("uid", "name", "namespace"):
        _require_workflow_string(metadata.get(field), location, f"metadata.{field}", True)
    labels = metadata.get("labels")
    if labels is not None:
        _require_workflow_mapping(labels, location, "metadata.labels")
        for key, value in labels.items():
            _require_workflow_string(value, location, f"metadata.labels[{key!r}]", True)

    spec = wf.get("spec")
    _require_workflow_mapping(spec, location, "spec")
    template_ref = spec.get("workflowTemplateRef")
    if template_ref is not None:
        _require_workflow_mapping(template_ref, location, "spec.workflowTemplateRef")
        _require_workflow_string(
            template_ref.get("name"), location, "spec.workflowTemplateRef.name", True
        )
        if "clusterScope" in template_ref and not isinstance(
            template_ref["clusterScope"], bool
        ):
            raise MalformedWorkflowError(
                f"{location}: 'spec.workflowTemplateRef.clusterScope' must be a boolean"
            )

    status = wf.get("status")
    if status is None:
        return
    _require_workflow_mapping(status, location, "status")
    for field in ("phase", "message", "progress", "startedAt", "finishedAt"):
        if field in status:
            _require_workflow_string(status[field], location, f"status.{field}")

    resources = status.get("resourcesDuration")
    if resources is not None:
        _require_workflow_mapping(resources, location, "status.resourcesDuration")
        for field in ("cpu", "memory"):
            if field in resources and (
                not isinstance(resources[field], int) or isinstance(resources[field], bool)
            ):
                raise MalformedWorkflowError(
                    f"{location}: 'status.resourcesDuration.{field}' must be an integer"
                )

    nodes = status.get("nodes")
    if nodes is not None:
        _require_workflow_mapping(nodes, location, "status.nodes")
        for node_name, node in nodes.items():
            _require_workflow_mapping(node, location, f"status.nodes[{node_name!r}]")
            for field in ("phase", "type", "displayName", "name", "startedAt", "message"):
                if field in node:
                    _require_workflow_string(
                        node[field], location, f"status.nodes[{node_name!r}].{field}"
                    )


def _parse_ts(value):
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        log.debug("unparseable timestamp: %r", value)
        return None


def normalize_timestamp(value):
    """Return a source timestamp as UTC RFC 3339, or None when unusable.

    Kubernetes normally supplies RFC 3339 timestamps with a timezone. Treat a
    missing, timezone-less, or malformed value as absent rather than allowing
    it to leak into the output or break duration calculation.
    """
    parsed = _parse_ts(value)
    if parsed is None or parsed.tzinfo is None:
        if parsed is not None:
            log.debug("timestamp has no timezone: %r", value)
        return None
    normalized = parsed.astimezone(timezone.utc).isoformat()
    return normalized.replace("+00:00", "Z")


def duration_seconds(started_at, finished_at):
    """Wall-clock seconds from start to finish, or None while a run is still
    going (or never started). Deliberately not "age so far" — a consumer can
    compute that from `started_at` and the snapshot's own timestamp, and
    conflating the two would make a running workflow indistinguishable from a
    finished one in the same column."""
    start, finish = _parse_ts(started_at), _parse_ts(finished_at)
    if start is not None and start.tzinfo is None:
        start = None
    if finish is not None and finish.tzinfo is None:
        finish = None
    if start is None or finish is None:
        return None
    return int(
        (
            finish.astimezone(timezone.utc) - start.astimezone(timezone.utc)
        ).total_seconds()
    )


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
    `user`, or (None, None) when nothing identifying was recorded.

    The labels can coexist on the same Workflow, so select the provenance
    source in one fixed order: cron, then Argo Events, then creator. An
    Events trigger label is only meaningful alongside its sensor label; a
    trigger-only Workflow therefore continues to the creator fallback.
    """
    labels = (wf.get("metadata") or {}).get("labels") or {}

    def non_empty_label(name):
        value = labels.get(name)
        return value if isinstance(value, str) and value.strip() else None

    cron = non_empty_label(LABEL_CRON)
    if cron is not None:
        return "cron", cron

    # An Argo Events sensor names both itself and the specific trigger within
    # it; the trigger is the more useful of the two because one sensor
    # commonly fans out to many.
    sensor = non_empty_label(LABEL_SENSOR)
    if sensor is not None:
        return "event", non_empty_label(LABEL_TRIGGER) or sensor

    creator = non_empty_label(LABEL_CREATOR)
    if creator is not None:
        return "user", creator
    return None, None


def _decode_compressed_nodes(value):
    """Decode Argo's base64-encoded gzip node map.

    ``compressedNodes`` is an optimization for large Workflows, not a second
    node shape: after decompression it contains the same JSON object that
    would otherwise be present in ``status.nodes``. A malformed value should
    not make an otherwise usable Workflow disappear, so return an empty map
    and let the workflow-level failure message remain the fallback.
    """
    if not isinstance(value, str) or not value:
        if value is not None:
            log.warning(
                "ignoring malformed status.compressedNodes: expected a non-empty string"
            )
        return {}

    try:
        compressed = base64.b64decode(value, validate=True)
        decoded = json.loads(gzip.decompress(compressed))
    except (binascii.Error, EOFError, OSError, TypeError, UnicodeError, ValueError) as exc:
        log.warning("ignoring malformed status.compressedNodes: %s", exc)
        return {}

    if not isinstance(decoded, dict):
        log.warning(
            "ignoring malformed status.compressedNodes: decoded JSON is %s, expected an object",
            type(decoded).__name__,
        )
        return {}

    # JSON object keys are strings, and valid Argo node values are objects.
    # Keep only usable entries so a malformed individual node cannot break
    # failure extraction for the rest of the Workflow.
    return {
        node_name: node
        for node_name, node in decoded.items()
        if isinstance(node_name, str) and isinstance(node, dict)
    }


def failed_step(wf):
    """`(display_name, message)` of the step that failed, or (None, None).

    Only pod nodes are considered: when a step fails, its parent DAG/steps
    nodes fail too, and reporting those would name the whole workflow back to
    itself instead of the thing that broke. Earliest failure wins — later
    ones are usually consequences of it.

    Argo replaces `status.nodes` with `status.compressedNodes` on very large
    workflows. Decode that field when the node map is absent; malformed
    compressed data is treated like a missing node map.
    """
    status = wf.get("status") or {}
    nodes = status.get("nodes")
    if nodes is None:
        nodes = _decode_compressed_nodes(status.get("compressedNodes"))
    if not isinstance(nodes, dict):
        nodes = {}
    failures = [
        n for n in nodes.values()
        if isinstance(n, dict)
        and n.get("phase") in _TERMINAL_NODE_PHASES
        and n.get("type") == "Pod"
    ]
    if not failures:
        return None, None
    earliest = min(
        failures,
        key=lambda n: n.get("startedAt") if isinstance(n.get("startedAt"), str) else "",
    )
    return earliest.get("displayName") or earliest.get("name"), earliest.get("message")


def to_row(wf, cluster_name: str, observed_at: str) -> dict:
    _validate_workflow(wf)
    meta = wf.get("metadata") or {}
    status = wf.get("status") or {}
    template, template_scope = template_of(wf)
    trigger_kind, trigger_name = trigger_of(wf)
    step_name, step_message = failed_step(wf)
    resources = status.get("resourcesDuration") or {}

    # The most specific thing the run said about why it failed. The step
    # message names the actual breakage ("exit code 137"); the workflow
    # message only names the step ("failed step 'build'"). When neither exists
    # the run did not fail, and both derived columns stay null.
    taxonomy = failure_columns(step_message, status.get("message"))

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
        "created_at": normalize_timestamp(meta.get("creationTimestamp")),
        "started_at": normalize_timestamp(status.get("startedAt")),
        "finished_at": normalize_timestamp(status.get("finishedAt")),
        "duration_seconds": duration_seconds(status.get("startedAt"), status.get("finishedAt")),
        # Argo's own accumulated resource counters. Only cpu and memory are
        # kept as columns; an installation using extended resources (GPUs,
        # ephemeral-storage) will find those keys dropped.
        "resources_duration_cpu": resources.get("cpu"),
        "resources_duration_memory": resources.get("memory"),
        "failed_step": step_name,
        "failed_step_message": step_message,
        **taxonomy,
        "observed_at": observed_at,
    }


def fetch_workflows(clusters, default_namespace: str, timeout: int, page_size: int, observed_at: str):
    """Returns `(rows, cluster_stats)` — one row per Workflow object that
    currently exists, across every reachable cluster.

    A cluster that fails, answers only partially, or returns malformed list or
    Workflow data contributes no rows and is reported `ok: false`, rather
    than contributing what did arrive. Consumers read a missing workflow as a
    deleted one, so a half-answer is worse than no answer: it would show runs
    vanishing that are still there. Malformed data is logged as an error and
    isolated to that cluster; other clusters can still produce a snapshot.
    """
    rows, stats = [], []
    for cluster in clusters:
        namespace = cluster.namespace if cluster.namespace is not None else default_namespace
        path = list_path(namespace, "workflows")
        try:
            items, complete = list_items(cluster, path, timeout, page_size)
        except KubernetesResponseError as exc:
            log.error(
                "%s: malformed Kubernetes response: %s; "
                "discarding this cluster for the cycle",
                cluster.name,
                exc,
            )
            stats.append({"name": cluster.name, "ok": False, "workflows": 0})
            continue
        if not complete:
            log.warning(
                "%s: incomplete workflow listing; discarding any partial items "
                "and skipping this cluster for this cycle",
                cluster.name,
            )
            stats.append({"name": cluster.name, "ok": False, "workflows": 0})
            continue

        cluster_rows = []
        try:
            for index, wf in enumerate(items):
                _validate_workflow(wf, index)
                cluster_rows.append(to_row(wf, cluster.name, observed_at))
        except MalformedWorkflowError as exc:
            log.error(
                "%s: malformed Workflow response: %s; "
                "discarding all %d item(s) for this cluster",
                cluster.name,
                exc,
                len(items),
            )
            stats.append({"name": cluster.name, "ok": False, "workflows": 0})
            continue
        except Exception as exc:
            # Keep an unexpected shape error isolated to its cluster too. The
            # exception text is logged so malformed data is surfaced rather
            # than becoming a mysteriously empty result.
            log.exception(
                "%s: could not convert Workflow response: %s; "
                "discarding all %d item(s) for this cluster",
                cluster.name,
                exc,
                len(items),
            )
            stats.append({"name": cluster.name, "ok": False, "workflows": 0})
            continue
        rows.extend(cluster_rows)
        stats.append({"name": cluster.name, "ok": True, "workflows": len(cluster_rows)})
        log.info("%s: %d workflow(s)", cluster.name, len(cluster_rows))
    return rows, stats
