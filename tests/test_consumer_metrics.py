import pytest

from src import consumer, parquet_io


def _publication(workflows, runs):
    return consumer.Publication(
        meta={},
        workflows=parquet_io.table_to_parquet_bytes(workflows, parquet_io.WORKFLOWS_SCHEMA),
        runs=parquet_io.table_to_parquet_bytes(runs, parquet_io.RUNS_SCHEMA),
    )


def _runs():
    return [
        {
            "uid": "success-1",
            "cluster": "ci",
            "phase": "Succeeded",
            "finished_at": "2026-09-26T10:00:00Z",
            "duration_seconds": 10,
            "first_seen_at": "2026-09-26T09:50:00Z",
            "last_seen_at": "2026-09-26T10:00:00Z",
        },
        {
            "uid": "success-2",
            "cluster": "ci",
            "phase": "Succeeded",
            "finished_at": "2026-09-26T11:00:00Z",
            "duration_seconds": 20,
            "first_seen_at": "2026-09-26T10:50:00Z",
            "last_seen_at": "2026-09-26T11:00:00Z",
        },
        {
            "uid": "failed",
            "cluster": "ci",
            "phase": "Failed",
            "finished_at": "2026-09-27T10:00:00Z",
            "duration_seconds": 30,
            "failure_class": "build",
            "failure_fingerprint": "build-1",
            "first_seen_at": "2026-09-27T09:50:00Z",
            "last_seen_at": "2026-09-27T10:00:00Z",
        },
        {
            "uid": "error",
            "cluster": "ci",
            "phase": "Error",
            "finished_at": "2026-09-27T11:00:00Z",
            "duration_seconds": 40,
            "failure_class": "timeout",
            "failure_fingerprint": "timeout-1",
            "first_seen_at": "2026-09-27T10:50:00Z",
            "last_seen_at": "2026-09-27T11:00:00Z",
        },
        {
            "uid": "running",
            "cluster": "ci",
            "phase": "Running",
            "last_seen_at": "2026-09-27T12:00:00Z",
            "first_seen_at": "2026-09-27T11:50:00Z",
        },
    ]


def test_historical_metrics_read_only_runs_parquet():
    runs = _runs()
    publication = consumer.Publication(
        meta={},
        # A historical query must not even try to decode this live snapshot.
        workflows=b"this is not a parquet file",
        runs=parquet_io.table_to_parquet_bytes(runs, parquet_io.RUNS_SCHEMA),
    )

    metrics = consumer.historical_metrics(publication)

    assert metrics["phase_counts"] == {
        "Succeeded": 2,
        "Failed": 1,
        "Error": 1,
        "Running": 1,
    }
    assert metrics["rates"] == {
        "total": 5,
        "completed": 4,
        "succeeded": 2,
        "failed": 2,
        "error": 1,
        "success_rate": 0.5,
        "failure_rate": 0.5,
        "completion_rate": 0.8,
    }
    assert metrics["failure_counts"] == {"build": 1, "timeout": 1}
    assert metrics["failure_fingerprint_counts"] == {"build-1": 1, "timeout-1": 1}
    assert [row["uid"] for row in metrics["duration_history"]] == [
        "success-1",
        "success-2",
        "failed",
        "error",
    ]
    assert metrics["trends"] == [
        {
            "period": "2026-09-26",
            "total": 2,
            "succeeded": 2,
            "failed": 0,
            "error": 0,
            "completed": 2,
            "success_rate": 1.0,
            "failure_rate": 0.0,
        },
        {
            "period": "2026-09-27",
            "total": 3,
            "succeeded": 0,
            "failed": 1,
            "error": 1,
            "completed": 2,
            "success_rate": 0.0,
            "failure_rate": 1.0,
        },
    ]
    assert consumer.historical_failure_counts(publication, "failure_fingerprint") == {
        "build-1": 1,
        "timeout-1": 1,
    }


def test_current_snapshot_reader_is_separate_from_historical_readers():
    publication = _publication(
        [{"uid": "live", "cluster": "ci", "phase": "Running"}],
        _runs(),
    )

    assert [row["uid"] for row in consumer.current_snapshot_rows(publication)] == ["live"]
    assert [row["uid"] for row in consumer.historical_run_rows(publication)] == [
        "success-1",
        "success-2",
        "failed",
        "error",
        "running",
    ]


@pytest.mark.parametrize("bucket", ["minute", "month", ""])  # unsupported historical views
def test_historical_trends_reject_unknown_bucket(bucket):
    publication = _publication([], _runs())

    with pytest.raises(ValueError, match="bucket must be one of"):
        consumer.historical_trends(publication, bucket)
