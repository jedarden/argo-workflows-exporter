"""Read paired exporter generations from object storage.

The exporter publishes ``workflows.parquet``, ``runs.parquet``, and
``meta.json`` with independent object writes.  A consumer must therefore make
the generation check before interpreting either Parquet payload.  This module
keeps downloaded payloads together as a :class:`Publication` and only returns
a candidate when the sidecar and both Parquet footers name the same
generation. Freshness is a separate check: a complete generation can be
 stale when repeated failed cycles leave all three objects unchanged.
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import isfinite
from typing import Any, Iterable, Mapping

from . import meta_schema, parquet_io, s3io

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Publication:
    """The three downloaded objects for one candidate publication.

    The Parquet payloads remain as bytes deliberately.  Callers can decode
    rows after :func:`select_generation` has established that all three
    objects belong to one generation.  A missing object is represented by
    ``None`` while a candidate is being checked, but a returned publication is
    always complete.
    """

    meta: Mapping[str, Any]
    workflows: bytes | None
    runs: bytes | None


# ``Generation`` is a useful name for callers that think of the selected
# publication as a generation rather than a set of objects.
Generation = Publication


def freshness_threshold_seconds(meta: Mapping[str, Any], max_cycle_seconds) -> float:
    """Return the freshness threshold derived from the effective cadence.

    ``max_cycle_seconds`` is the consumer's configured upper bound for one
    exporter cycle (``C_max``).  The publisher's post-cycle delay is carried
    in ``meta.poll_interval_seconds``, so the effective cadence is
    ``C_max + POLL_INTERVAL_SECONDS`` rather than a fixed timeout.
    """

    if (
        isinstance(max_cycle_seconds, bool)
        or not isinstance(max_cycle_seconds, (int, float))
        or not isfinite(max_cycle_seconds)
        or max_cycle_seconds < 0
    ):
        raise ValueError("max_cycle_seconds must be a finite non-negative number")

    poll_interval = meta.get("poll_interval_seconds")
    if isinstance(poll_interval, bool) or not isinstance(poll_interval, int) or poll_interval < 1:
        raise ValueError("meta.poll_interval_seconds must be a positive integer")
    return float(max_cycle_seconds + poll_interval)


def freshness_age_seconds(meta: Mapping[str, Any], now: datetime) -> float:
    """Return the UTC age of ``meta.generated_at`` at ``now``."""

    if not isinstance(now, datetime) or now.tzinfo is None:
        raise ValueError("now must be a timezone-aware datetime")
    generated_at = meta.get("generated_at")
    if not isinstance(generated_at, str):
        raise ValueError("meta.generated_at must be a UTC timestamp string")
    try:
        generated = datetime.strptime(generated_at, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise ValueError("meta.generated_at must be a UTC timestamp string") from exc
    return (now.astimezone(timezone.utc) - generated).total_seconds()


def is_fresh(meta: Mapping[str, Any], now: datetime, max_cycle_seconds) -> bool:
    """Whether a sidecar is younger than one effective polling cadence.

    Equality is stale.  Once a failed cycle could have completed, the old
    heartbeat is no longer evidence that current data exists.
    """

    return freshness_age_seconds(meta, now) < freshness_threshold_seconds(
        meta, max_cycle_seconds
    )


def generation_ids(
    publication: Publication | Mapping[str, Any],
) -> dict[str, str | None]:
    """Return the sidecar and footer generation ids without reading rows."""

    meta = _publication_value(publication, "meta")
    meta_id = meta.get("generation_id") if isinstance(meta, Mapping) else None
    if not isinstance(meta_id, str) or not meta_id:
        meta_id = None

    return {
        "meta": meta_id,
        "workflows": _footer_generation_id(_publication_value(publication, "workflows")),
        "runs": _footer_generation_id(_publication_value(publication, "runs")),
    }


def _publication_value(
    publication: Publication | Mapping[str, Any], name: str
) -> Any:
    if isinstance(publication, Mapping):
        return publication.get(name)
    return getattr(publication, name, None)


def _valid_meta(meta: Any) -> bool:
    """Whether metadata is a complete, internally consistent sidecar.

    ``read_generation`` validates the decoded object before downloading the
    Parquet payloads. The other consumer helpers are also public entry points,
    though, and callers can construct a :class:`Publication` without going
    through storage. Apply the same boundary check there so matching footer
    IDs cannot make malformed metadata look committed.
    """

    try:
        meta_schema.validate(meta)
    except (TypeError, ValueError, meta_schema.MetaSchemaError) as exc:
        log.warning("rejecting publication with invalid meta.json: %s", exc)
        return False
    return True


def cluster_availability(
    publication: Publication | Mapping[str, Any],
) -> dict[str, bool]:
    """Return the current snapshot availability for every cluster.

    ``ok: false`` means that no current snapshot was obtained for the
    cluster.  It is deliberately kept separate from the cluster's included
    workflow count: a failed cluster reports zero included rows, but that
    zero is not confirmation that the cluster is empty and must not be used
    to delete a consumer's last-known rows.  The sidecar has already been
    schema-validated when it came through :func:`read_generation`; callers
    using a hand-built publication get a clear error for malformed cluster
    metadata instead of a misleading status map.
    """

    meta = _publication_value(publication, "meta")
    if not isinstance(meta, Mapping):
        raise ValueError("publication.meta must be a mapping")
    clusters = meta.get("clusters")
    if not isinstance(clusters, list):
        raise ValueError("publication.meta.clusters must be a list")

    availability = {}
    for index, cluster in enumerate(clusters):
        if not isinstance(cluster, Mapping):
            raise ValueError(f"publication.meta.clusters[{index}] must be a mapping")
        name = cluster.get("name")
        ok = cluster.get("ok")
        if not isinstance(name, str) or not name:
            raise ValueError(f"publication.meta.clusters[{index}].name must be non-empty")
        if not isinstance(ok, bool):
            raise ValueError(f"publication.meta.clusters[{index}].ok must be a boolean")
        if name in availability:
            raise ValueError(f"publication.meta.clusters contains duplicate name {name!r}")
        availability[name] = ok
    return availability


def _footer_generation_id(data: bytes | None) -> str | None:
    if data is None:
        return None
    try:
        return parquet_io.read_generation_id(data)
    except Exception as exc:
        # A corrupt or non-Parquet object is no safer to interpret than a
        # missing object.  Keep the previous paired generation and let the
        # next publication repair the object set.
        log.warning("could not read a Parquet generation footer: %s", exc)
        return None


def is_complete_generation(
    publication: Publication | Mapping[str, Any] | None,
) -> bool:
    """Whether the sidecar and both data objects form one valid generation.

    Zero-row Parquet files are valid generations because their identity is
    stored in file metadata rather than in a row, but a matching footer ID is
    not enough to commit malformed sidecar metadata.
    """

    if publication is None:
        return False

    if not _valid_meta(_publication_value(publication, "meta")):
        return False

    ids = generation_ids(publication)
    values = tuple(ids.values())
    if not all(value is not None for value in values) or len(set(values)) != 1:
        return False

    # The schema validator requires this relationship too. Keep this explicit
    # check beside the footer comparison so the identity rule remains clear at
    # the point where a publication becomes eligible for selection.
    meta = _publication_value(publication, "meta")
    return values[0].startswith(f"{meta['generated_at']}-")


def select_generation(
    candidate: Publication | Mapping[str, Any] | None,
    last_complete: Publication | Mapping[str, Any] | None,
    *,
    max_cycle_seconds=None,
    now: datetime | None = None,
) -> Publication | Mapping[str, Any] | None:
    """Select a candidate or retain the last complete publication.

    A failed, missing, torn, or stale candidate is never returned.  When
    ``max_cycle_seconds`` is supplied, freshness is checked against the
    effective cadence derived from the candidate sidecar.  The previous value
    is assumed to have come from an earlier successful call, so it is returned
    by identity and is not reinterpreted or mixed with candidate payloads.
    """

    if candidate is None or not is_complete_generation(candidate):
        ids = generation_ids(candidate) if candidate is not None else None
        if ids is not None and len(set(ids.values())) > 1:
            log.warning(
                "rejecting inconsistent publication generation ids: "
                "meta=%r workflows=%r runs=%r; retaining last complete generation",
                ids["meta"],
                ids["workflows"],
                ids["runs"],
            )
        return last_complete
    if max_cycle_seconds is not None and not is_fresh(
        _publication_value(candidate, "meta"),
        datetime.now(timezone.utc) if now is None else now,
        max_cycle_seconds,
    ):
        return last_complete
    return candidate


def _key(prefix: str, name: str) -> str:
    return s3io.object_key(prefix, name)


def read_generation(
    s3,
    bucket: str,
    prefix: str,
    last_complete: Publication | None = None,
    *,
    max_cycle_seconds=None,
    now: datetime | None = None,
) -> Publication | None:
    """Read and select the newest paired publication from S3-compatible storage.

    ``meta.json`` is fetched first and acts as the commit marker.  Only after
    it is present, decodable, and fresh are the two Parquet objects fetched;
    their complete payloads are then checked using footer metadata before this
    function returns anything.  If any object is missing, malformed, stale, or
    has a different generation id, ``last_complete`` is retained.  Pass the
    consumer's ``C_max`` as ``max_cycle_seconds`` to enable the freshness
    check; without it this function provides only the pairing check.

    A missing ``meta.json`` with no ``last_complete`` is the normal bootstrap
    state before the first successful exporter cycle: ``None`` means that no
    generation has been published yet.  It is not an empty generation or a
    storage error, and no Parquet objects are fetched or interpreted in that
    case.  If the marker exists but either data object is missing, the same
    no-generation result is returned when there is no prior complete
    publication.

    Storage errors other than a missing object still propagate: retrying an
    unavailable store is different from silently presenting stale data, and
    the caller can decide how to report that operational failure.
    """

    meta_bytes = s3io.download_bytes(s3, bucket, _key(prefix, "meta.json"))
    if meta_bytes is None:
        return last_complete

    try:
        meta = json.loads(meta_bytes)
        meta_schema.validate(meta)
    except (TypeError, ValueError, UnicodeDecodeError, meta_schema.MetaSchemaError) as exc:
        log.warning("ignoring an invalid meta.json candidate: %s", exc)
        return last_complete

    if max_cycle_seconds is not None:
        checked_at = datetime.now(timezone.utc) if now is None else now
        age = freshness_age_seconds(meta, checked_at)
        threshold = freshness_threshold_seconds(meta, max_cycle_seconds)
        if age >= threshold:
            log.warning(
                "stale meta.json candidate: generation_id=%s age_seconds=%.3f "
                "freshness_threshold_seconds=%.3f; retaining last complete generation",
                meta["generation_id"],
                age,
                threshold,
            )
            return last_complete

    # Keep this order explicit: the marker is read before either data object,
    # then both footers are available for one consistency decision.
    workflows = s3io.download_bytes(s3, bucket, _key(prefix, "workflows.parquet"))
    runs = s3io.download_bytes(s3, bucket, _key(prefix, "runs.parquet"))
    candidate = Publication(meta=meta, workflows=workflows, runs=runs)
    return select_generation(candidate, last_complete)


# ``load_generation`` reads naturally at call sites and keeps the storage
# operation discoverable without making two public implementations.
load_generation = read_generation
read_publication = read_generation


# These are the phase groups used by the historical metric helpers below.
# ``workflows.parquet`` is intentionally not involved in any of them: it is a
# point-in-time snapshot and TTL deletes make it a biased historical sample.
_SUCCESS_PHASE = "Succeeded"
_FAILURE_PHASES = frozenset(("Failed", "Error"))
_TERMINAL_PHASES = frozenset((_SUCCESS_PHASE, *_FAILURE_PHASES))
_TREND_BUCKETS = frozenset(("hour", "day", "week"))


def _rows(publication: Publication | Mapping[str, Any], name: str, schema):
    data = _publication_value(publication, name)
    return parquet_io.parquet_bytes_to_table(data, schema).to_pylist()


def current_snapshot_rows(publication: Publication | Mapping[str, Any]):
    """Decode the current-state rows from ``workflows.parquet``.

    This is the only row reader in this module that uses the snapshot file.
    Callers should use it for live inventory views, never for rates, trends,
    duration history, or outcome counts.  A partial snapshot contains rows
    only for clusters that completed their listing, but apply the sidecar's
    availability map here as well so a malformed or hand-built publication
    cannot turn an unavailable cluster's zero count into an empty inventory.
    """

    rows = _rows(publication, "workflows", parquet_io.WORKFLOWS_SCHEMA)
    meta = _publication_value(publication, "meta")
    if not isinstance(meta, Mapping) or "clusters" not in meta:
        # Metadata-less publications are useful for callers that only need
        # schema decoding; read_generation validates the real sidecar before
        # this helper is used for a published inventory.
        return rows

    available_clusters = {
        name for name, available in cluster_availability(publication).items() if available
    }
    return [row for row in rows if row.get("cluster") in available_clusters]


def historical_run_rows(publication: Publication | Mapping[str, Any]):
    """Decode retained run state from ``runs.parquet``.

    The ledger has one current row per observed run and outlives the Workflow
    object that produced it. Every historical metric helper is built on this
    reader so a TTL-biased live listing cannot silently affect its result.
    """

    return _rows(publication, "runs", parquet_io.RUNS_SCHEMA)


def _phase_counts(rows: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    return dict(Counter(row.get("phase") for row in rows if row.get("phase") is not None))


def historical_phase_counts(publication: Publication | Mapping[str, Any]):
    """Count workflow phases using the retained run ledger."""

    return _phase_counts(historical_run_rows(publication))


def _rate(numerator: int, denominator: int):
    return numerator / denominator if denominator else None


def _historical_rates(rows: list[Mapping[str, Any]]):
    counts = _phase_counts(rows)
    succeeded = counts.get(_SUCCESS_PHASE, 0)
    failed = counts.get("Failed", 0)
    errors = counts.get("Error", 0)
    failures = failed + errors
    terminal = sum(counts.get(phase, 0) for phase in _TERMINAL_PHASES)
    return {
        "total": len(rows),
        "completed": terminal,
        "succeeded": succeeded,
        "failed": failures,
        "error": errors,
        "success_rate": _rate(succeeded, terminal),
        "failure_rate": _rate(failures, terminal),
        "completion_rate": _rate(terminal, len(rows)),
    }


def historical_rates(publication: Publication | Mapping[str, Any]):
    """Return outcome rates calculated from ``runs.parquet``.

    Success and failure rates use terminal runs as their denominator, so a
    retained ``Running`` row does not make a completed-run rate look worse.
    ``completion_rate`` reports the share of all retained rows that are
    terminal.  Empty populations return ``None`` for rates rather than
    manufacturing a zero-valued measurement.
    """

    return _historical_rates(historical_run_rows(publication))


def _timestamp(row: Mapping[str, Any]):
    # Finished time is the most useful event time for a run trend. Older or
    # incomplete rows may not have it, so fall back to the ledger timestamps
    # rather than dropping a retained run from every trend query.
    for field in ("finished_at", "last_seen_at", "first_seen_at"):
        value = row.get(field)
        if not isinstance(value, str):
            continue
        try:
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            continue
    return None


def _trend_period(value: datetime, bucket: str):
    if bucket == "hour":
        return value.strftime("%Y-%m-%dT%H:00:00Z")
    if bucket == "day":
        return value.strftime("%Y-%m-%d")
    week_start = value - timedelta(days=value.weekday())
    return week_start.strftime("%Y-%m-%d")


def _historical_trends(rows: list[Mapping[str, Any]], bucket: str):
    if bucket not in _TREND_BUCKETS:
        choices = ", ".join(sorted(_TREND_BUCKETS))
        raise ValueError(f"bucket must be one of: {choices}")

    grouped = {}
    for row in rows:
        timestamp = _timestamp(row)
        if timestamp is None:
            continue
        period = _trend_period(timestamp, bucket)
        entry = grouped.setdefault(
            period,
            {"period": period, "total": 0, "succeeded": 0, "failed": 0, "error": 0},
        )
        entry["total"] += 1
        phase = row.get("phase")
        if phase == _SUCCESS_PHASE:
            entry["succeeded"] += 1
        elif phase == "Failed":
            entry["failed"] += 1
        elif phase == "Error":
            entry["error"] += 1

    trends = []
    for period in sorted(grouped):
        entry = grouped[period]
        terminal = entry["succeeded"] + entry["failed"] + entry["error"]
        entry["completed"] = terminal
        entry["success_rate"] = _rate(entry["succeeded"], terminal)
        entry["failure_rate"] = _rate(entry["failed"] + entry["error"], terminal)
        trends.append(entry)
    return trends


def historical_trends(
    publication: Publication | Mapping[str, Any], bucket: str = "day"
):
    """Group historical outcome rates by finish/ledger time from ``runs.parquet``."""

    return _historical_trends(historical_run_rows(publication), bucket)


def _historical_duration_history(rows: list[Mapping[str, Any]]):
    history = []
    for row in rows:
        duration = row.get("duration_seconds")
        if isinstance(duration, bool) or not isinstance(duration, (int, float)):
            continue
        history.append(
            {
                "cluster": row.get("cluster"),
                "uid": row.get("uid"),
                "name": row.get("name"),
                "phase": row.get("phase"),
                "started_at": row.get("started_at"),
                "finished_at": row.get("finished_at"),
                "duration_seconds": duration,
            }
        )
    history.sort(
        key=lambda row: (
            row.get("finished_at") or row.get("started_at") or "",
            row.get("cluster") or "",
            row.get("uid") or "",
        )
    )
    return history


def historical_duration_history(publication: Publication | Mapping[str, Any]):
    """Return completed-run durations retained in ``runs.parquet`` order."""

    return _historical_duration_history(historical_run_rows(publication))


def historical_failure_counts(
    publication: Publication | Mapping[str, Any], field: str = "failure_class"
):
    """Count failure classes or fingerprints from retained failed runs.

    ``field`` may be ``failure_class`` (the dashboard-friendly grouping) or
    ``failure_fingerprint`` (the normalized error grouping). Missing taxonomy
    data is grouped under ``unknown`` instead of silently dropping a failure.
    """

    if field not in {"failure_class", "failure_fingerprint"}:
        raise ValueError("field must be failure_class or failure_fingerprint")
    return _failure_counts(historical_run_rows(publication), field)


def _failure_counts(rows: Iterable[Mapping[str, Any]], field: str):
    counts = Counter()
    for row in rows:
        if row.get("phase") not in _FAILURE_PHASES:
            continue
        counts[row.get(field) or "unknown"] += 1
    return dict(sorted(counts.items()))


def historical_metrics(publication: Publication | Mapping[str, Any]):
    """Return all historical dashboard measures from one ledger decode.

    The returned mapping keeps current inventory separate: use
    :func:`current_snapshot_rows` for that view. No field in this result is
    derived from ``workflows.parquet``.
    """

    rows = historical_run_rows(publication)
    return {
        "rates": _historical_rates(rows),
        "phase_counts": _phase_counts(rows),
        "trends": _historical_trends(rows, "day"),
        "duration_history": _historical_duration_history(rows),
        "failure_counts": _failure_counts(rows, "failure_class"),
        "failure_fingerprint_counts": _failure_counts(rows, "failure_fingerprint"),
    }


# Short names for consumers that already have a publication selected. Keep
# the source-explicit names above as the canonical API and these aliases as a
# convenience, not as alternate data-loading paths.
snapshot_rows = current_snapshot_rows
run_rows = historical_run_rows
rates = historical_rates
trends = historical_trends
duration_history = historical_duration_history
failure_counts = historical_failure_counts
