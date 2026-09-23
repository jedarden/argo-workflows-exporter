"""The meta.json contract: field names, types, and the rules tying them
together.

meta.json is the commit marker (docs/notes/atomic-publication.md) — the
object consumers read first and the one that says a generation is complete.
A writer regression here does not just break a file, it misdescribes a
generation that exists, so the publisher validates every sidecar against
this module before uploading it, and consumers are invited to do the same.

The per-field structure lives in META_SCHEMA, a JSON Schema document. The
keywords it uses are a common subset (`format: rfc3339-utc-second` is this
exporter's own annotation); `validate` interprets them directly, with no
outside dependency, and then applies the cross-field rules a schema
language cannot state: the id must extend the timestamp it is paired with,
the per-cluster counts must sum to the top-level count, and a cluster that
did not answer must report zero. Specified field by field in
docs/notes/output-schema.md.
"""

import re
from datetime import datetime

# `_now()` publishes second-resolution RFC 3339 UTC with a literal Z — see
# its comment in main.py for why the suffix is not optional. The regex checks
# the shape; the strptime in validate() rejects shapes that are not real
# calendar times (2026-02-30 passes the regex, not the parser).
_TIMESTAMP = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z"

# `<generated_at>-<12 lowercase hex>`: the same timestamp plus the random
# suffix that keeps two cycles landing in one second distinct. Lowercase
# because `uuid4().hex` is, and a consumer comparing ids byte-wise should
# never meet a case surprise.
_GENERATION_ID = _TIMESTAMP + r"-[0-9a-f]{12}"

CLUSTER_SCHEMA = {
    "type": "object",
    "required": ["name", "ok", "workflows"],
    "additionalProperties": False,
    "properties": {
        "name": {"type": "string", "minLength": 1},
        # A strict boolean, not "truthy": JSON distinguishes true from 1,
        # and a consumer filtering on `ok` should never learn otherwise.
        "ok": {"type": "boolean"},
        "workflows": {"type": "integer", "minimum": 0},
    },
}

META_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "argo-workflows-exporter meta.json",
    "type": "object",
    "required": [
        "version",
        "generated_at",
        "generation_id",
        "poll_interval_seconds",
        "run_retention_days",
        "clusters",
        "workflows",
        "runs",
    ],
    "additionalProperties": False,
    "properties": {
        "version": {"type": "string", "minLength": 1},
        "generated_at": {"type": "string", "format": "rfc3339-utc-second"},
        "generation_id": {"type": "string", "pattern": _GENERATION_ID},
        "poll_interval_seconds": {"type": "integer", "minimum": 1},
        "run_retention_days": {"type": "integer", "minimum": 1},
        "clusters": {"type": "array", "minItems": 1, "items": CLUSTER_SCHEMA},
        "workflows": {"type": "integer", "minimum": 0},
        "runs": {"type": "integer", "minimum": 0},
    },
}


class MetaSchemaError(Exception):
    """meta.json violates its contract; str(err) lists every violation."""


def _is_integer(value):
    # bool subclasses int in Python but not in JSON: `true` is not 1.
    return isinstance(value, int) and not isinstance(value, bool)


_TYPE_CHECKS = {
    "object": lambda v: isinstance(v, dict),
    "array": lambda v: isinstance(v, list),
    "string": lambda v: isinstance(v, str),
    "integer": _is_integer,
    "boolean": lambda v: isinstance(v, bool),
}


def _describe(value):
    if isinstance(value, str):
        return f"string {value!r}"
    if value is None:
        return "null"
    return f"{type(value).__name__} {value!r}"


def _check(value, rule, where, errors):
    if not _TYPE_CHECKS[rule["type"]](value):
        errors.append(f"{where} must be a JSON {rule['type']}, got {_describe(value)}")
        return

    if rule["type"] == "object":
        for key in rule.get("required", []):
            if key not in value:
                errors.append(f"{where} is missing required field {key!r}")
        for key in value:
            if key not in rule.get("properties", {}):
                errors.append(f"{where} has unexpected field {key!r}")
        for key, sub in rule.get("properties", {}).items():
            if key in value:
                _check(value[key], sub, f"{where}.{key}", errors)

    elif rule["type"] == "array":
        minimum = rule.get("minItems", 0)
        if len(value) < minimum:
            errors.append(f"{where} must have at least {minimum} item(s), got {len(value)}")
        for i, item in enumerate(value):
            _check(item, rule["items"], f"{where}[{i}]", errors)

    elif rule["type"] == "string":
        if len(value) < rule.get("minLength", 1):
            errors.append(f"{where} must not be empty")
        if "pattern" in rule and not re.fullmatch(rule["pattern"], value):
            errors.append(f"{where} must match /{rule['pattern']}/ exactly, got {value!r}")
        if rule.get("format") == "rfc3339-utc-second":
            try:
                datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                errors.append(f"{where} is shaped like a timestamp but not a real UTC time: {value!r}")

    elif rule["type"] == "integer":
        if "minimum" in rule and value < rule["minimum"]:
            errors.append(f"{where} must be >= {rule['minimum']}, got {value}")


def _cross_field_violations(meta):
    """The rules META_SCHEMA cannot state. Each guards a claim the docs make
    to consumers (docs/notes/output-schema.md), so the sidecar is published
    only when every claim it makes about the generation holds together."""
    errors = []
    if not isinstance(meta, dict):
        return errors

    generated_at = meta.get("generated_at")
    generation_id = meta.get("generation_id")
    if isinstance(generated_at, str) and isinstance(generation_id, str):
        if not generation_id.startswith(f"{generated_at}-"):
            errors.append(
                f"meta.generation_id must extend meta.generated_at: {generation_id!r} "
                f"does not start with {generated_at + '-'!r}"
            )

    clusters = meta.get("clusters")
    total = meta.get("workflows")
    if isinstance(clusters, list) and _is_integer(total):
        names = [c.get("name") for c in clusters if isinstance(c, dict)]
        duplicated = sorted({n for n in names if names.count(n) > 1})
        if duplicated:
            errors.append(
                f"meta.clusters has duplicate names {duplicated}; "
                "one entry per configured cluster, no repeats"
            )

        accounted = sum(
            c["workflows"]
            for c in clusters
            if isinstance(c, dict) and c.get("ok") is True and _is_integer(c.get("workflows"))
        )
        if accounted != total:
            errors.append(
                f"meta.workflows must equal the sum of the ok clusters' workflows: "
                f"clusters account for {accounted}, sidecar says {total}"
            )

        for i, c in enumerate(clusters):
            if (
                isinstance(c, dict)
                and c.get("ok") is False
                and _is_integer(c.get("workflows"))
                and c["workflows"] != 0
            ):
                errors.append(
                    f"meta.clusters[{i}] ({c.get('name')!r}) is ok=false and must report "
                    f"workflows=0 -- that count is rows included, not an emptiness claim"
                )
    return errors


def validate(meta):
    """Raises MetaSchemaError unless `meta` (a decoded meta.json) is exactly
    a publishable sidecar. Every violation is collected into one message, so
    a single failed validation shows the whole damage."""
    errors = []
    _check(meta, META_SCHEMA, "meta", errors)
    errors.extend(_cross_field_violations(meta))
    if errors:
        raise MetaSchemaError(
            "meta.json violates its schema:\n  " + "\n  ".join(errors)
        )
