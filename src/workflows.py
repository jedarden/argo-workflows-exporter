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


def to_row(wf, cluster_name: str, observed_at: str) -> dict:
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
    failure_message = step_message or status.get("message")
    _, fingerprint = normalize_failure(failure_message)

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
        "failure_fingerprint": fingerprint,
        "failure_class": failure_class(failure_message),
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
