from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pytest

from src.ledger import merge
from src.workflows import normalize_failure, to_row


_WORKFLOW_CASES = json.loads(
    (Path(__file__).with_name("fixtures") / "workflow_cases.json").read_text(
        encoding="utf-8"
    )
)


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
    # Now, not a fixed date: retention trims anything last seen outside the
    # window, so a hardcoded timestamp silently expires and the row never
    # comes back.
    now = _ts(0)
    [row] = merge([], [_observed()], now, 7)
    assert row["first_seen_at"] == now
    assert row["last_seen_at"] == now
    # observed_at belongs to the snapshot, not the ledger.
    assert "observed_at" not in row


def test_first_observation_derives_the_shared_failure_taxonomy():
    message = "error[E0432]: unresolved import crate::workflows"
    observed = _observed(
        phase="Failed",
        message="failed step 'build'",
        failed_step="build",
        failed_step_message=message,
        # The ledger must derive these from evidence instead of trusting a
        # caller's omitted or stale values.
        failure_fingerprint=None,
        failure_class="unknown",
    )

    [row] = merge([], [observed], _ts(0), 7)

    assert row["failure_class"] == "build"
    assert row["failure_fingerprint"] == normalize_failure(message)[1]


@pytest.mark.parametrize(
    ("fixture_name", "expected"),
    [
        pytest.param("progress_populated", "7/12", id="populated"),
        pytest.param("progress_absent", None, id="absent-is-null"),
        pytest.param("progress_empty", "", id="empty-is-verbatim"),
    ],
)
def test_ledger_folds_progress_without_coercion(fixture_name, expected):
    observed = to_row(
        _WORKFLOW_CASES[fixture_name], "ci", "2026-09-23T13:00:00Z"
    )

    [row] = merge([], [observed], _ts(0), 7)

    assert row["progress"] == expected


@pytest.mark.parametrize(
    ("phase", "finished_at", "duration_seconds"),
    [
        ("Running", None, None),
        ("Succeeded", "2026-09-23T11:01:02Z", 62),
        ("Failed", "2026-09-23T11:00:31Z", 31),
        ("Error", "2026-09-23T11:00:09Z", 9),
    ],
)
def test_reobservation_keeps_first_seen_and_takes_the_newer_state(
    phase, finished_at, duration_seconds
):
    first_at = _ts(1)
    last_at = _ts(0)
    first = merge([], [_observed(phase="Running")], first_at, 7)
    [row] = merge(
        first,
        [
            _observed(
                phase=phase,
                message=f"state: {phase}",
                finished_at=finished_at,
                duration_seconds=duration_seconds,
            )
        ],
        last_at,
        7,
    )

    assert row["first_seen_at"] == first_at
    assert row["last_seen_at"] == last_at
    assert row["phase"] == phase
    assert row["message"] == f"state: {phase}"
    assert row["finished_at"] == finished_at
    assert row["duration_seconds"] == duration_seconds


@pytest.mark.parametrize("phase", ["Running", "Succeeded", "Failed", "Error"])
def test_an_absent_running_or_terminal_run_is_preserved_unchanged(phase):
    """Argo's TTL deletes the object; the ledger is what outlives it."""
    stored = merge([], [_observed(uid="reaped", phase=phase)], _ts(0), 7)
    kept = merge(stored, [_observed(uid="still-here")], _ts(0), 7)

    [reaped] = [row for row in kept if row["uid"] == "reaped"]
    assert reaped == stored[0]


def test_rows_expire_a_retention_window_after_they_were_last_seen():
    stale = {"uid": "old", "phase": "Succeeded", "first_seen_at": _ts(40), "last_seen_at": _ts(30)}
    fresh = {"uid": "new", "phase": "Succeeded", "first_seen_at": _ts(1), "last_seen_at": _ts(1)}

    kept = merge([stale, fresh], [], _ts(0), 7)
    assert [r["uid"] for r in kept] == ["new"]


@pytest.mark.parametrize("phase", ["Running", "Succeeded"])
@pytest.mark.parametrize(
    ("last_seen_at", "retained"),
    [
        ("2026-09-16T11:59:59Z", False),
        ("2026-09-16T12:00:00Z", True),
    ],
)
def test_retention_is_measured_from_last_observation_at_the_cutoff(
    monkeypatch, phase, last_seen_at, retained
):
    monkeypatch.setattr("src.ledger._cutoff", lambda retention_days: "2026-09-16T12:00:00Z")
    old_run = {
        "uid": "old-run",
        "cluster": "ci",
        "phase": phase,
        "first_seen_at": "2026-08-01T12:00:00Z",
        "last_seen_at": last_seen_at,
    }

    kept = merge([old_run], [], "2026-09-23T12:00:00Z", 7)

    assert (kept == [old_run]) is retained


def test_a_long_running_workflow_is_not_expired_while_still_observed():
    # cluster is on the row because it is on every real ledger row -- the
    # shared schema has carried it since the first release.
    old_start = {
        "uid": "long",
        "cluster": "ci",
        "phase": "Running",
        "first_seen_at": _ts(30),
        "last_seen_at": _ts(30),
    }
    kept = merge([old_start], [_observed(uid="long", phase="Running")], _ts(0), 7)
    assert [r["uid"] for r in kept] == ["long"]
    assert kept[0]["first_seen_at"] == _ts(30)


def test_rows_without_a_uid_are_dropped_not_appended_every_cycle():
    rows = merge([], [_observed(uid="")], "2026-08-11T04:00:00Z", 7)
    assert rows == []


def test_identical_uids_from_different_clusters_are_two_runs():
    """Kubernetes scopes uid uniqueness to one cluster; two clusters minting
    the same uid are two runs, and the ledger must keep both rows."""
    now = _ts(0)
    kept = merge(
        [],
        [_observed(uid="shared"), _observed(uid="shared", cluster="prod")],
        now,
        7,
    )
    assert sorted((r["cluster"], r["uid"]) for r in kept) == [("ci", "shared"), ("prod", "shared")]
    assert all(r["first_seen_at"] == now for r in kept)


def test_an_observation_updates_only_its_own_cluster_row():
    first = merge(
        [],
        [_observed(uid="shared"), _observed(uid="shared", cluster="prod")],
        _ts(1),
        7,
    )
    kept = merge(
        first,
        [_observed(uid="shared", phase="Succeeded", duration_seconds=9)],
        _ts(0),
        7,
    )

    [ci] = [r for r in kept if r["cluster"] == "ci"]
    [prod] = [r for r in kept if r["cluster"] == "prod"]
    assert ci["phase"] == "Succeeded"
    assert prod["phase"] == "Running"
    # Each keeps the first_seen_at of its own first observation.
    assert ci["first_seen_at"] == prod["first_seen_at"] == _ts(1)


def test_same_name_with_different_uids_keeps_and_updates_both_runs():
    first_seen_at = _ts(2)
    second_seen_at = _ts(1)
    updated_at = _ts(0)

    first_run = merge(
        [],
        [_observed(uid="run-1", phase="Running")],
        first_seen_at,
        7,
    )
    both_runs = merge(
        first_run,
        [_observed(uid="run-2", phase="Succeeded", duration_seconds=12)],
        second_seen_at,
        7,
    )
    kept = merge(
        both_runs,
        [_observed(uid="run-1", phase="Failed", message="run 1 failed")],
        updated_at,
        7,
    )

    rows = {(row["cluster"], row["uid"]): row for row in kept}
    assert set(rows) == {("ci", "run-1"), ("ci", "run-2")}
    assert rows["ci", "run-1"]["name"] == rows["ci", "run-2"]["name"]
    assert rows["ci", "run-1"]["phase"] == "Failed"
    assert rows["ci", "run-1"]["message"] == "run 1 failed"
    assert rows["ci", "run-1"]["first_seen_at"] == first_seen_at
    assert rows["ci", "run-1"]["last_seen_at"] == updated_at
    assert rows["ci", "run-2"]["phase"] == "Succeeded"
    assert rows["ci", "run-2"]["duration_seconds"] == 12
    assert rows["ci", "run-2"]["first_seen_at"] == second_seen_at
    assert rows["ci", "run-2"]["last_seen_at"] == second_seen_at


def test_retention_is_decided_per_cluster_and_uid():
    """One cluster's stale run must neither carry nor drop the same-uid run
    in another cluster."""
    stale = {
        "uid": "shared",
        "cluster": "gone",
        "phase": "Failed",
        "first_seen_at": _ts(40),
        "last_seen_at": _ts(30),
    }
    fresh = {
        "uid": "shared",
        "cluster": "ci",
        "phase": "Running",
        "first_seen_at": _ts(1),
        "last_seen_at": _ts(1),
    }
    kept = merge([stale, fresh], [], _ts(0), 7)
    assert [(r["cluster"], r["uid"]) for r in kept] == [("ci", "shared")]


def test_removed_cluster_rows_follow_natural_retention(monkeypatch):
    """Stopping observations for a removed config entry does not purge history early."""
    cutoff = "2026-09-16T12:00:00Z"
    monkeypatch.setattr("src.ledger._cutoff", lambda retention_days: cutoff)
    removed_retained = {
        "uid": "removed-retained",
        "cluster": "removed",
        "phase": "Failed",
        "first_seen_at": "2026-09-10T10:00:00Z",
        "last_seen_at": cutoff,
    }
    removed_expired = {
        "uid": "removed-expired",
        "cluster": "removed",
        "phase": "Succeeded",
        "first_seen_at": "2026-09-09T10:00:00Z",
        "last_seen_at": "2026-09-16T11:59:59Z",
    }

    kept = merge(
        [removed_retained, removed_expired],
        [],
        "2026-09-23T12:00:00Z",
        7,
    )

    assert [(row["cluster"], row["uid"]) for row in kept] == [
        ("removed", "removed-retained")
    ]
    assert kept[0]["last_seen_at"] == cutoff


def test_renamed_cluster_starts_a_new_identity_and_keeps_old_history():
    old_seen_at = _ts(1)
    new_seen_at = _ts(0)
    old_name = merge(
        [],
        [_observed(uid="same-workflow", cluster="old")],
        old_seen_at,
        7,
    )

    kept = merge(
        old_name,
        [_observed(uid="same-workflow", cluster="new")],
        new_seen_at,
        7,
    )

    rows = {(row["cluster"], row["uid"]): row for row in kept}
    assert set(rows) == {("old", "same-workflow"), ("new", "same-workflow")}
    assert rows["old", "same-workflow"]["first_seen_at"] == old_seen_at
    assert rows["old", "same-workflow"]["last_seen_at"] == old_seen_at
    assert rows["new", "same-workflow"]["first_seen_at"] == new_seen_at
    assert rows["new", "same-workflow"]["last_seen_at"] == new_seen_at


def test_output_is_ordered_deterministically():
    rows = [
        {"uid": "b", "first_seen_at": "2026-08-11T02:00:00Z", "last_seen_at": _ts(0)},
        {"uid": "a", "first_seen_at": "2026-08-11T01:00:00Z", "last_seen_at": _ts(0)},
    ]
    assert [r["uid"] for r in merge(rows, [], _ts(0), 7)] == ["a", "b"]


def test_rows_sharing_a_first_seen_order_by_cluster_then_uid():
    rows = [
        {"uid": "b", "cluster": "prod", "first_seen_at": _ts(1), "last_seen_at": _ts(0)},
        {"uid": "a", "cluster": "prod", "first_seen_at": _ts(1), "last_seen_at": _ts(0)},
        {"uid": "z", "cluster": "ci", "first_seen_at": _ts(1), "last_seen_at": _ts(0)},
    ]
    assert [(r["cluster"], r["uid"]) for r in merge(rows, [], _ts(0), 7)] == [
        ("ci", "z"),
        ("prod", "a"),
        ("prod", "b"),
    ]
