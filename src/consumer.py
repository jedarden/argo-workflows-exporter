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
from dataclasses import dataclass
from datetime import datetime, timezone
from math import isfinite
from typing import Any, Mapping

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
    """Whether all three objects carry one non-empty generation id.

    This is intentionally a footer-only check.  In particular, zero-row
    Parquet files are valid generations because their identity is stored in
    file metadata rather than in a row.
    """

    if publication is None:
        return False

    ids = generation_ids(publication)
    values = tuple(ids.values())
    return all(value is not None for value in values) and len(set(values)) == 1


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

    if not is_complete_generation(candidate):
        return last_complete
    if max_cycle_seconds is not None and not is_fresh(
        _publication_value(candidate, "meta"),
        datetime.now(timezone.utc) if now is None else now,
        max_cycle_seconds,
    ):
        return last_complete
    return candidate


def _key(prefix: str, name: str) -> str:
    prefix = prefix.rstrip("/")
    return f"{prefix}/{name}" if prefix else name


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
