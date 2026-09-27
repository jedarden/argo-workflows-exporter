"""Shared failure evidence normalization and taxonomy derivation.

Both Parquet outputs contain the same failure columns. Keeping their
derivation here means a snapshot row and the corresponding ledger row cannot
classify one observation differently, even when the ledger is fed rows by a
caller other than the normal collection path.
"""

import hashlib
import re
from pathlib import Path

import yaml


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


def _usable_message(message):
    return message if isinstance(message, str) and message.strip() else None


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
    message = _usable_message(message)
    if message is None:
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
    message = _usable_message(message)
    if message is None:
        return None
    for name, patterns in _FAILURE_RULES:
        if any(pattern.search(message) for pattern in patterns):
            return name
    return _FAILURE_CLASS_UNKNOWN


def failure_columns(step_message, workflow_message):
    """Return the shared failure columns for one workflow observation.

    A failed pod's message is more specific than Argo's workflow-level
    message, but compressed node trees and workflow-level failures have only
    the latter. Blank evidence is treated as absent so a malformed or empty
    step message cannot mask useful workflow-level evidence.
    """
    message = _usable_message(step_message) or _usable_message(workflow_message)
    _, fingerprint = normalize_failure(message)
    return {
        "failure_fingerprint": fingerprint,
        "failure_class": failure_class(message),
    }
