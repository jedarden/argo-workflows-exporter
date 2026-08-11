from datetime import datetime, timedelta, timezone

from src.ledger import merge


def _observed(uid="uid-1", phase="Running", **extra):
    row = {
        "uid": uid,
        "cluster": "ci",
        "namespace": "argo",
        "name": "example-build-abcde",
        "phase": phase,
        "observed_at": "2026-08-11T04:00:00Z",
    }
    row.update(extra)
    return row


def _ts(days_ago):
    return (datetime.now(timezone.utc) - timedelta(days=days_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")


def test_first_observation_stamps_both_timestamps():
    [row] = merge([], [_observed()], "2026-08-11T04:00:00Z", 7)
    assert row["first_seen_at"] == "2026-08-11T04:00:00Z"
    assert row["last_seen_at"] == "2026-08-11T04:00:00Z"
    # observed_at belongs to the snapshot, not the ledger.
    assert "observed_at" not in row


def test_reobservation_keeps_first_seen_and_takes_the_newer_state():
    first = merge([], [_observed(phase="Running")], _ts(1), 7)
    [row] = merge(first, [_observed(phase="Succeeded", duration_seconds=62)], _ts(0), 7)

    assert row["first_seen_at"] == first[0]["first_seen_at"]
    assert row["last_seen_at"] == _ts(0)
    assert row["phase"] == "Succeeded"
    assert row["duration_seconds"] == 62


def test_a_run_absent_this_cycle_is_retained_at_its_last_known_state():
    """Argo's TTL deletes the object; the ledger is what outlives it."""
    stored = merge([], [_observed(uid="reaped", phase="Succeeded")], _ts(0), 7)
    kept = merge(stored, [_observed(uid="still-here")], _ts(0), 7)

    reaped = [r for r in kept if r["uid"] == "reaped"]
    assert len(reaped) == 1
    assert reaped[0]["phase"] == "Succeeded"


def test_rows_expire_a_retention_window_after_they_were_last_seen():
    stale = {"uid": "old", "phase": "Succeeded", "first_seen_at": _ts(40), "last_seen_at": _ts(30)}
    fresh = {"uid": "new", "phase": "Succeeded", "first_seen_at": _ts(1), "last_seen_at": _ts(1)}

    kept = merge([stale, fresh], [], _ts(0), 7)
    assert [r["uid"] for r in kept] == ["new"]


def test_a_long_running_workflow_is_not_expired_while_still_observed():
    old_start = {"uid": "long", "phase": "Running", "first_seen_at": _ts(30), "last_seen_at": _ts(30)}
    kept = merge([old_start], [_observed(uid="long", phase="Running")], _ts(0), 7)
    assert [r["uid"] for r in kept] == ["long"]
    assert kept[0]["first_seen_at"] == _ts(30)


def test_rows_without_a_uid_are_dropped_not_appended_every_cycle():
    rows = merge([], [_observed(uid="")], "2026-08-11T04:00:00Z", 7)
    assert rows == []


def test_output_is_ordered_deterministically():
    rows = [
        {"uid": "b", "first_seen_at": "2026-08-11T02:00:00Z", "last_seen_at": _ts(0)},
        {"uid": "a", "first_seen_at": "2026-08-11T01:00:00Z", "last_seen_at": _ts(0)},
    ]
    assert [r["uid"] for r in merge(rows, [], _ts(0), 7)] == ["a", "b"]
