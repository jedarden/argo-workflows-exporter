"""The run ledger: an append-and-update record of every workflow this
exporter has ever seen, keyed by UID.

This exists because a live listing is not a record of what ran. Argo's
`ttlStrategy` deletes completed Workflow objects — commonly within minutes,
and typically *sooner for successes than for failures* — so a snapshot taken
at any moment is biased towards whatever fails and lingers. A cluster whose
runs are mostly green can present a listing that is mostly red, purely
because the green ones were collected first.

The ledger fixes that by remembering each run past the deletion of the object
it came from. Its accuracy therefore depends on the poll interval being
comfortably shorter than the shortest TTL in effect: a run that starts and is
reaped entirely between two polls is never observed and never recorded. See
`docs/notes/ttl-and-observation-windows.md`.
"""

import logging
from datetime import datetime, timedelta, timezone

log = logging.getLogger(__name__)


def _cutoff(retention_days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=retention_days)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def merge(existing_rows, observed_rows, generated_at: str, retention_days: int):
    """Folds this cycle's observations into the stored ledger and trims it.

    A UID already present keeps its original `first_seen_at` and takes every
    other field from the new observation — a run's phase, duration and
    message all change as it progresses, and the latest observation is the
    truthful one. A UID that is absent this cycle is left untouched: it has
    almost certainly been deleted by Argo's TTL, and its last observed state
    is exactly what we want to keep.

    Retention is measured from `last_seen_at`, not from when the run started.
    That keeps a long-running workflow in the ledger for as long as it is
    alive however long that is, and expires a finished one a fixed window
    after it stopped being observable.

    Rows are held as dicts rather than Arrow arrays throughout: the ledger is
    bounded by (runs per day x retention days), which is thousands of rows at
    the scale this is built for, not millions.
    """
    by_uid = {row["uid"]: row for row in existing_rows if row.get("uid")}

    updated = 0
    for observed in observed_rows:
        uid = observed.get("uid")
        if not uid:
            # Every object the API server returns has one; a row without one
            # cannot be tracked across cycles, so it is dropped rather than
            # appended as a duplicate on every poll.
            log.warning("skipping workflow with no uid: %s", observed.get("name"))
            continue

        row = {k: v for k, v in observed.items() if k != "observed_at"}
        previous = by_uid.get(uid)
        row["first_seen_at"] = previous["first_seen_at"] if previous else generated_at
        row["last_seen_at"] = generated_at
        by_uid[uid] = row
        updated += 1

    cutoff = _cutoff(retention_days)
    kept = [r for r in by_uid.values() if (r.get("last_seen_at") or "") >= cutoff]
    dropped = len(by_uid) - len(kept)
    if dropped:
        log.info("trimmed %d run(s) last seen before %s", dropped, cutoff)

    # Stable ordering keeps the written object byte-comparable between cycles
    # when nothing changed, and groups each run's history together on disk.
    kept.sort(key=lambda r: (r.get("first_seen_at") or "", r.get("uid") or ""))
    log.info("ledger: %d run(s) after merging %d observation(s)", len(kept), updated)
    return kept
