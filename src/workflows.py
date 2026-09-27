"""Turns raw Argo `Workflow` objects into flat rows.

Everything here reads only fields upstream Argo itself sets, so the same
extraction works against any Argo Workflows installation.
"""

import hashlib
import logging
import re
from datetime import datetime
from pathlib import Path

import yaml

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


# Instance-specific tokens a failure message carries. Two runs that failed for
# the same reason produce messages differing only in these; replacing them with
# placeholders is what makes their fingerprints equal. Order matters: a rule
# must run before a coarser one could eat its match (timestamps before paths,
# pod names before bare numbers).
_NORMALIZATIONS = (
    # `0195a1d2-93e5-7c41-9f0e-2b6f1c8d4a77`
    (re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b"), "<uuid>"),
    # `2026-09-06T03:31:30Z`, `2026-09-06 03:31:30.123456+00:00`
    (re.compile(
        r"\b\d{4}-\d{2}-\d{2}[ t]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:z|[+-]\d{2}:?\d{2})?\b"
    ), "<ts>"),
    # `https://git.ardenone.com/jedarden/perch.git` — before paths, whose rule
    # would otherwise match only the path half and leave the host behind.
    (re.compile(r"\bhttps?://[^\s'\")\]}]+"), "<url>"),
    # `/home/coding/argo-workflows-exporter/src/main.py:42` — two or more
    # segments, so `/bin/sh` and single component names survive.
    (re.compile(r"(?<![\w/])(?:/[a-z0-9_.@+-]+){2,}/?(?::\d+)?"), "<path>"),
    # `300ms`, `2.5s`, `1h2m3s`, `0.00s` — Go-style compound durations included.
    (re.compile(
        r"\b\d+(?:\.\d+)?(?:ms|us|µs|ns|h|m|s)(?:\d+(?:\.\d+)?(?:ms|us|µs|ns|h|m|s))*\b"
    ), "<dur>"),
    # `rust-verify-7gk2m`, `build-abcde-1234567890-x9z2k` — at least three
    # dash-separated segments with a Kubernetes-generated five character
    # suffix, so a hand-written name like `argo-workflows-exporter` is untouched.
    (re.compile(r"\b[a-z0-9]+(?:-[a-z0-9]+){1,6}-[a-z0-9]{5}\b"), "<pod>"),
    # `k3s-worker-01.ec2.internal`, `git.ardenone.com`
    (re.compile(r"\b[a-z0-9][a-z0-9-]*(?:\.[a-z0-9-]+)+\.[a-z]{2,}\b"), "<node>"),
    # `10.96.0.1`
    (re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b"), "<ip>"),
    # `9f86d081884c7d65`, `d6e0715` — a hex run of hash length carrying at
    # least one digit, so ordinary words made only of a-f letters are not eaten.
    (re.compile(r"\b(?=[0-9a-f]*[0-9])[0-9a-f]{7,}\b"), "<hash>"),
    # `65535`, `2026`
    (re.compile(r"\b\d{4,}\b"), "<n>"),
)

_FAILURE_CLASS_UNKNOWN = "unknown"
_FAILURE_CLASSES_FILE = Path(__file__).with_name("failure_classes.yaml")


def _load_failure_classes(path=_FAILURE_CLASSES_FILE):
    """Reads and compiles the rule table once, at import.

    A malformed rule fails startup rather than quietly classifying everything
    `unknown` — the same fail-fast position config.py takes, and for the same
    reason: a taxonomy that silently stops working looks identical to a fleet
    that has stopped failing.
    """
    with open(path) as f:
        entries = yaml.safe_load(f) or []
    if not isinstance(entries, list):
        raise ValueError(f"{path}: expected a list of rules")
    rules = []
    for entry in entries:
        name, patterns = entry.get("class"), entry.get("patterns") or []
        if name == _FAILURE_CLASS_UNKNOWN:
            raise ValueError(f"{path}: 'unknown' is the fallback and takes no rules")
        try:
            compiled = [re.compile(p, re.IGNORECASE) for p in patterns]
        except re.error as e:
            raise ValueError(f"{path}: bad pattern in {name} rule: {e}") from e
        rules.append((name, compiled))
    return rules


_FAILURE_RULES = _load_failure_classes()


def normalize_failure(message):
    """`(normalized_text, fingerprint)` for a failure message, or
    `(None, None)` when there is no message.

    The text is lowercased, every instance-specific token is replaced by a
    placeholder (see `_NORMALIZATIONS`) and whitespace is collapsed; the
    fingerprint is the first 12 hex characters of that text's SHA-256. It is
    stable across releases by construction — no salt, no versioning — because
    its whole purpose is joining failures observed at different times, so a
    re-derivation that produced new values would silently break every such
    join already in flight.
    """
    if not message or not message.strip():
        return None, None
    text = message.lower()
    for pattern, replacement in _NORMALIZATIONS:
        text = pattern.sub(replacement, text)
    text = re.sub(r"\s+", " ", text).strip()
    return text, hashlib.sha256(text.encode()).hexdigest()[:12]


def failure_class(message):
    """The first matching class from `failure_classes.yaml`, `unknown` when
    nothing matches, or None when there is no message to classify.

    Runs against the raw message, not the normalized text: normalization
    erases exactly the tokens — exit codes, durations, image tags, paths —
    that distinguish a build failure from a test one.
    """
    if not message or not message.strip():
        return None
    for name, patterns in _FAILURE_RULES:
        if any(pattern.search(message) for pattern in patterns):
            return name
    return _FAILURE_CLASS_UNKNOWN


def failure_columns(step_message, workflow_message):
    """Return the shared failure columns for one workflow observation.

    A failed pod's message is more specific than Argo's workflow-level
    message, but compressed node trees and workflow-level failures have only
    the latter. Keeping the fallback and both null cases in one function makes
    the values copied to the snapshot and ledger rows impossible to diverge.
    """
    message = step_message or workflow_message
    _, fingerprint = normalize_failure(message)
    return {
        "failure_fingerprint": fingerprint,
        "failure_class": failure_class(message),
    }


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
