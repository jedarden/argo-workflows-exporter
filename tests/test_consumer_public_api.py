"""Compatibility tests for the documented public consumer API.

These tests intentionally use the names and call shapes from
``docs/notes/consumer-api.md``.  The lower-level generation-consistency tests
exercise the storage contract; this module protects callers from accidental
changes to the public aliases, return shape, row views, and metric helpers.
"""

import json

import pyarrow as pa
import pytest

from src import consumer, parquet_io


GENERATED_AT = "2026-09-27T12:00:00Z"
GENERATION_ID = f"{GENERATED_AT}-111111aaaaaa"


def _meta(*, clusters=None, workflows=None, runs=None):
    if clusters is None:
        clusters = [{"name": "ci", "ok": True, "workflows": 1}]
    if workflows is None:
        workflows = sum(cluster["workflows"] for cluster in clusters if cluster["ok"])
    if runs is None:
        runs = 1
    return {
        "version": "public-api-test",
        "generated_at": GENERATED_AT,
        "generation_id": GENERATION_ID,
        "poll_interval_seconds": 300,
        "run_retention_days": 7,
        "clusters": clusters,
        "workflows": workflows,
        "runs": runs,
    }


def _published_bytes():
    workflows = parquet_io.table_to_parquet_bytes(
        [{"uid": "live", "cluster": "ci", "phase": "Running"}],
        parquet_io.WORKFLOWS_SCHEMA,
        GENERATION_ID,
    )
    runs = parquet_io.table_to_parquet_bytes(
        [{"uid": "run-1", "cluster": "ci", "phase": "Succeeded"}],
        parquet_io.RUNS_SCHEMA,
        GENERATION_ID,
    )
    return workflows, runs


def _stored_objects(meta, workflows, runs):
    return {
        "argo/data/meta.json": json.dumps(meta).encode(),
        "argo/data/workflows.parquet": workflows,
        "argo/data/runs.parquet": runs,
    }


def test_generation_is_the_documented_publication_alias_and_shape():
    meta = _meta()
    workflows = b"raw workflows bytes"

    publication = consumer.Generation(meta=meta, workflows=workflows, runs=None)

    assert consumer.Generation is consumer.Publication
    assert isinstance(publication, consumer.Publication)
    assert publication.meta is meta
    assert publication.workflows is workflows
    assert publication.runs is None


@pytest.mark.parametrize(
    "loader",
    [
        pytest.param(consumer.read_generation, id="read_generation"),
        pytest.param(consumer.load_generation, id="load_generation"),
        pytest.param(consumer.read_publication, id="read_publication"),
    ],
)
def test_public_loaders_return_decoded_meta_and_raw_parquet_bytes(loader, monkeypatch):
    meta = _meta()
    workflows, runs = _published_bytes()
    objects = _stored_objects(meta, workflows, runs)

    monkeypatch.setattr(
        consumer.s3io,
        "download_bytes",
        lambda _s3, _bucket, key: objects.get(key),
    )

    selected = loader(object(), "bucket", "argo/data")

    assert type(selected) is consumer.Publication
    assert selected.meta == meta
    assert isinstance(selected.meta, dict)
    assert selected.workflows is workflows
    assert selected.runs is runs
    assert selected.workflows == objects["argo/data/workflows.parquet"]
    assert selected.runs == objects["argo/data/runs.parquet"]


def test_public_loaders_are_aliases_of_read_generation():
    assert consumer.load_generation is consumer.read_generation
    assert consumer.read_publication is consumer.read_generation


@pytest.mark.parametrize(
    "loader",
    [
        pytest.param(consumer.read_generation, id="read_generation"),
        pytest.param(consumer.load_generation, id="load_generation"),
        pytest.param(consumer.read_publication, id="read_publication"),
    ],
)
def test_public_loaders_preserve_bootstrap_none_and_last_complete(loader, monkeypatch):
    previous = consumer.Publication(meta={"previous": True}, workflows=b"old", runs=b"old")
    calls = []

    def download(_s3, _bucket, key):
        calls.append(key)
        return None

    monkeypatch.setattr(consumer.s3io, "download_bytes", download)

    assert loader(object(), "bucket", "argo/data") is None
    assert loader(object(), "bucket", "argo/data", previous) is previous
    assert calls == [
        "argo/data/meta.json",
        "argo/data/meta.json",
    ]


def test_missing_data_is_none_or_the_unchanged_previous_publication(monkeypatch):
    meta = _meta()
    workflows, runs = _published_bytes()
    objects = _stored_objects(meta, workflows, runs)
    calls = []

    def download(_s3, _bucket, key):
        calls.append(key)
        return objects.get(key)

    monkeypatch.setattr(consumer.s3io, "download_bytes", download)
    assert consumer.read_generation(object(), "bucket", "argo/data") is not None

    objects["argo/data/runs.parquet"] = None
    calls.clear()
    assert consumer.read_generation(object(), "bucket", "argo/data") is None

    previous = consumer.Publication(meta=meta, workflows=workflows, runs=runs)
    assert consumer.read_generation(object(), "bucket", "argo/data", previous) is previous
    assert calls == [
        "argo/data/meta.json",
        "argo/data/workflows.parquet",
        "argo/data/runs.parquet",
        "argo/data/meta.json",
        "argo/data/workflows.parquet",
        "argo/data/runs.parquet",
    ]


def test_public_row_helpers_decode_and_normalize_older_files():
    old_workflows_schema = pa.schema(
        [
            ("uid", pa.string()),
            ("cluster", pa.string()),
            ("phase", pa.string()),
            ("retired_column", pa.string()),
        ]
    )
    old_runs_schema = pa.schema(
        [
            ("uid", pa.string()),
            ("cluster", pa.string()),
            ("phase", pa.string()),
            ("finished_at", pa.string()),
            ("duration_seconds", pa.int64()),
            ("retired_column", pa.string()),
        ]
    )
    publication = consumer.Publication(
        meta=_meta(),
        workflows=parquet_io.table_to_parquet_bytes(
            [
                {
                    "uid": "legacy-workflow",
                    "cluster": "ci",
                    "phase": "Running",
                    "retired_column": "discard me",
                }
            ],
            old_workflows_schema,
        ),
        runs=parquet_io.table_to_parquet_bytes(
            [
                {
                    "uid": "legacy-run",
                    "cluster": "ci",
                    "phase": "Succeeded",
                    "finished_at": "2026-09-27T12:00:00Z",
                    "duration_seconds": 42,
                    "retired_column": "discard me",
                }
            ],
            old_runs_schema,
        ),
    )

    [workflow] = consumer.current_snapshot_rows(publication)
    [run] = consumer.historical_run_rows(publication)

    assert workflow["uid"] == "legacy-workflow"
    assert workflow["failure_class"] is None
    assert workflow["observed_at"] is None
    assert "retired_column" not in workflow
    assert run["uid"] == "legacy-run"
    assert run["duration_seconds"] == 42
    assert run["failure_fingerprint"] is None
    assert run["first_seen_at"] is None
    assert "retired_column" not in run


def test_current_inventory_helper_and_alias_respect_cluster_availability():
    meta = _meta(
        clusters=[
            {"name": "ci", "ok": True, "workflows": 1},
            {"name": "staging", "ok": False, "workflows": 0},
        ],
        workflows=1,
        runs=2,
    )
    publication = consumer.Publication(
        meta=meta,
        workflows=parquet_io.table_to_parquet_bytes(
            [
                {"uid": "current", "cluster": "ci", "phase": "Running"},
                {"uid": "unavailable", "cluster": "staging", "phase": "Running"},
            ],
            parquet_io.WORKFLOWS_SCHEMA,
        ),
        runs=parquet_io.table_to_parquet_bytes([], parquet_io.RUNS_SCHEMA),
    )

    assert consumer.snapshot_rows is consumer.current_snapshot_rows
    assert consumer.cluster_availability(publication) == {"ci": True, "staging": False}
    assert [row["uid"] for row in consumer.current_snapshot_rows(publication)] == [
        "current"
    ]
    assert [row["uid"] for row in consumer.snapshot_rows(publication)] == ["current"]


def _historical_rows():
    return [
        {
            "uid": "success",
            "cluster": "ci",
            "phase": "Succeeded",
            "finished_at": "2026-09-26T10:00:00Z",
            "duration_seconds": 10,
        },
        {
            "uid": "failed",
            "cluster": "ci",
            "phase": "Failed",
            "finished_at": "2026-09-27T10:00:00Z",
            "duration_seconds": 20,
            "failure_class": "build",
            "failure_fingerprint": "build-1",
        },
        {
            "uid": "error",
            "cluster": "ci",
            "phase": "Error",
            "finished_at": "2026-09-27T11:00:00Z",
            "duration_seconds": 30,
            "failure_class": "timeout",
            "failure_fingerprint": "timeout-1",
        },
        {
            "uid": "running",
            "cluster": "ci",
            "phase": "Running",
            "last_seen_at": "2026-09-27T12:00:00Z",
        },
    ]


def test_documented_historical_helpers_and_short_aliases_use_the_run_ledger():
    publication = consumer.Publication(
        meta={},
        # Historical helpers must not fall back to this current snapshot.
        workflows=b"not parquet",
        runs=parquet_io.table_to_parquet_bytes(
            _historical_rows(), parquet_io.RUNS_SCHEMA
        ),
    )

    assert consumer.run_rows is consumer.historical_run_rows
    assert consumer.rates is consumer.historical_rates
    assert consumer.trends is consumer.historical_trends
    assert consumer.duration_history is consumer.historical_duration_history
    assert consumer.failure_counts is consumer.historical_failure_counts

    assert [row["uid"] for row in consumer.run_rows(publication)] == [
        "success",
        "failed",
        "error",
        "running",
    ]
    assert consumer.historical_phase_counts(publication) == {
        "Succeeded": 1,
        "Failed": 1,
        "Error": 1,
        "Running": 1,
    }
    assert consumer.rates(publication) == {
        "total": 4,
        "completed": 3,
        "succeeded": 1,
        "failed": 2,
        "error": 1,
        "success_rate": 1 / 3,
        "failure_rate": 2 / 3,
        "completion_rate": 3 / 4,
    }
    assert consumer.trends(publication) == [
        {
            "period": "2026-09-26",
            "total": 1,
            "succeeded": 1,
            "failed": 0,
            "error": 0,
            "completed": 1,
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
    assert [row["uid"] for row in consumer.duration_history(publication)] == [
        "success",
        "failed",
        "error",
    ]
    assert consumer.failure_counts(publication) == {"build": 1, "timeout": 1}
    assert consumer.historical_failure_counts(
        publication, field="failure_fingerprint"
    ) == {"build-1": 1, "timeout-1": 1}

    metrics = consumer.historical_metrics(publication)
    assert metrics["phase_counts"] == consumer.historical_phase_counts(publication)
    assert metrics["rates"] == consumer.historical_rates(publication)
    assert metrics["trends"] == consumer.historical_trends(publication)
    assert metrics["duration_history"] == consumer.historical_duration_history(publication)
    assert metrics["failure_counts"] == consumer.historical_failure_counts(publication)
    assert metrics["failure_fingerprint_counts"] == consumer.historical_failure_counts(
        publication, field="failure_fingerprint"
    )


def test_row_helpers_decode_missing_objects_as_empty_views():
    publication = consumer.Publication(meta={}, workflows=None, runs=None)

    assert consumer.current_snapshot_rows(publication) == []
    assert consumer.historical_run_rows(publication) == []
    assert consumer.historical_phase_counts(publication) == {}
    assert consumer.historical_rates(publication) == {
        "total": 0,
        "completed": 0,
        "succeeded": 0,
        "failed": 0,
        "error": 0,
        "success_rate": None,
        "failure_rate": None,
        "completion_rate": None,
    }
