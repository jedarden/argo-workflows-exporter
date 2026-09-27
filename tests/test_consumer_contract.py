"""Executable consumer fixtures for the generation-consistency contract.

The exporter publishes three independent objects. These tests materialize the
fixture rows as real Parquet bytes, then exercise the consumer's required
footer-only pairing check before any rows are interpreted. A torn candidate is
not mixed with the last complete generation.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src import consumer, meta_schema, parquet_io


FIXTURE_PATH = Path(__file__).with_name("fixtures") / "generation_consistency.json"
_C_MAX_SECONDS = 60
_AT_STALE_BOUNDARY = datetime(2026, 9, 27, 11, 56, tzinfo=timezone.utc)


def _load_fixtures():
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _materialize(case):
    """Build the three downloaded objects represented by one fixture case."""
    return consumer.Publication(
        meta=case["meta"],
        workflows=parquet_io.table_to_parquet_bytes(
            case["workflows"]["rows"],
            parquet_io.WORKFLOWS_SCHEMA,
            case["workflows"]["generation_id"],
        ),
        runs=parquet_io.table_to_parquet_bytes(
            case["runs"]["rows"],
            parquet_io.RUNS_SCHEMA,
            case["runs"]["generation_id"],
        ),
    )


@pytest.fixture(scope="module")
def fixtures():
    return _load_fixtures()


@pytest.mark.parametrize(
    "case_name",
    [
        "complete",
        "torn",
        "zero_row",
        "failed_clusters",
        "unreachable_cluster",
        "partial_pagination_failure",
        "recovered_cluster",
        "stale",
        "pre_generation_id",
    ],
)
def test_fixture_sidecars_and_both_parquet_footers_are_executable(fixtures, case_name):
    case = fixtures[case_name]
    publication = _materialize(case)

    meta_schema.validate(publication.meta)
    ids = consumer.generation_ids(publication)
    assert ids["workflows"] == case["workflows"]["generation_id"]
    assert ids["runs"] == case["runs"]["generation_id"]
    assert consumer.is_complete_generation(publication) is case["expected"]["accepted"]


def test_pre_generation_id_fixture_retains_the_last_complete_generation(fixtures):
    previous = _materialize(fixtures["complete"])
    legacy = _materialize(fixtures["pre_generation_id"])

    assert consumer.generation_ids(legacy) == {
        "meta": "2026-09-27T11:55:00Z-666666ffffff",
        "workflows": None,
        "runs": None,
    }
    assert consumer.select_generation(legacy, previous) is previous


def test_torn_fixture_retains_the_last_complete_generation(fixtures):
    complete = _materialize(fixtures["complete"])
    torn = _materialize(fixtures["torn"])

    assert consumer.generation_ids(torn) == {
        "meta": "2026-09-27T12:00:00Z-111111aaaaaa",
        "workflows": "2026-09-27T12:01:00Z-222222bbbbbb",
        "runs": "2026-09-27T12:00:00Z-111111aaaaaa",
    }
    selected = consumer.select_generation(torn, complete)
    assert selected is complete
    assert selected.meta["generation_id"] == fixtures["complete"]["meta"]["generation_id"]

    # The fallback is what a consumer presents; it must not leak the torn
    # snapshot's rows into a generation whose marker still names the old one.
    selected_rows = parquet_io.parquet_bytes_to_table(
        selected.workflows, parquet_io.WORKFLOWS_SCHEMA
    ).to_pylist()
    assert [row["uid"] for row in selected_rows] == ["wf-complete"]


@pytest.mark.parametrize(
    "case_name",
    [
        "zero_row",
        "failed_clusters",
        "unreachable_cluster",
        "partial_pagination_failure",
        "recovered_cluster",
    ],
)
def test_valid_non_torn_fixtures_are_selected_and_preserve_snapshot_semantics(
    fixtures, case_name
):
    case = fixtures[case_name]
    publication = _materialize(case)
    selected = consumer.select_generation(publication, _materialize(fixtures["complete"]))

    assert selected is publication
    workflows = parquet_io.parquet_bytes_to_table(
        selected.workflows, parquet_io.WORKFLOWS_SCHEMA
    ).to_pylist()
    runs = parquet_io.parquet_bytes_to_table(
        selected.runs, parquet_io.RUNS_SCHEMA
    ).to_pylist()
    expected = case["expected"]
    assert [row["uid"] for row in workflows] == expected["workflow_uids"]
    assert [row["uid"] for row in runs] == expected["run_uids"]
    assert consumer.cluster_availability(publication) == expected["cluster_availability"]
    assert {
        name for name, available in consumer.cluster_availability(publication).items() if not available
    } == set(expected["unavailable_clusters"])
    assert {row["cluster"] for row in workflows} == set(expected["workflow_clusters"])
    assert {row["cluster"] for row in runs} == set(expected["run_clusters"])

    if case_name == "zero_row":
        assert workflows == []
        assert runs == []


@pytest.mark.parametrize("case_name", ["unreachable_cluster", "partial_pagination_failure"])
def test_failed_cluster_zero_count_is_unavailable_not_empty_or_deleted(fixtures, case_name):
    publication = _materialize(fixtures[case_name])
    expected = fixtures[case_name]["expected"]

    assert expected["unavailable_clusters"] == ["staging"]
    assert consumer.cluster_availability(publication)["staging"] is False
    assert next(
        cluster["workflows"]
        for cluster in publication.meta["clusters"]
        if cluster["name"] == "staging"
    ) == 0

    workflows = parquet_io.parquet_bytes_to_table(
        publication.workflows, parquet_io.WORKFLOWS_SCHEMA
    ).to_pylist()
    runs = parquet_io.parquet_bytes_to_table(
        publication.runs, parquet_io.RUNS_SCHEMA
    ).to_pylist()
    assert all(row["cluster"] != "staging" for row in workflows)
    assert any(row["cluster"] == "staging" for row in runs)


def test_failed_cluster_recovers_in_a_later_generation(fixtures):
    failed = _materialize(fixtures["partial_pagination_failure"])
    recovered = _materialize(fixtures["recovered_cluster"])

    assert consumer.select_generation(recovered, failed) is recovered
    assert consumer.cluster_availability(failed)["staging"] is False
    assert consumer.cluster_availability(recovered)["staging"] is True

    workflows = parquet_io.parquet_bytes_to_table(
        recovered.workflows, parquet_io.WORKFLOWS_SCHEMA
    ).to_pylist()
    assert {row["cluster"] for row in workflows} == {"ci", "staging"}
    assert {row["uid"] for row in workflows} == {
        "wf-ci-recovered",
        "wf-staging-recovered",
    }


def test_stale_fixture_is_complete_but_fails_the_effective_cadence_policy(fixtures):
    publication = _materialize(fixtures["stale"])
    meta = publication.meta

    assert consumer.is_complete_generation(publication)
    assert fixtures["stale"]["expected"]["fresh"] is False
    assert consumer.freshness_threshold_seconds(meta, _C_MAX_SECONDS) == 360
    assert consumer.freshness_age_seconds(meta, _AT_STALE_BOUNDARY) == 360
    assert consumer.is_fresh(meta, _AT_STALE_BOUNDARY, _C_MAX_SECONDS) is False
    assert consumer.is_fresh(
        meta, datetime(2026, 9, 27, 11, 55, 59, tzinfo=timezone.utc), _C_MAX_SECONDS
    ) is True
    previous = _materialize(fixtures["complete"])
    assert (
        consumer.select_generation(
            publication,
            previous,
            max_cycle_seconds=_C_MAX_SECONDS,
            now=_AT_STALE_BOUNDARY,
        )
        is previous
    )


def test_stale_fixture_holds_last_complete_generation_and_alerts(
    fixtures, monkeypatch, caplog
):
    previous = _materialize(fixtures["complete"])
    stale = _materialize(fixtures["stale"])
    objects = _stored_objects(stale)
    calls = []

    def download(_s3, _bucket, key):
        calls.append(key)
        return objects.get(key)

    monkeypatch.setattr(consumer.s3io, "download_bytes", download)
    caplog.set_level("WARNING", logger="src.consumer")

    selected = consumer.read_generation(
        object(),
        "bucket",
        "argo/data",
        previous,
        max_cycle_seconds=_C_MAX_SECONDS,
        now=datetime(2026, 9, 27, 12, 6, tzinfo=timezone.utc),
    )

    assert selected is previous
    assert selected.meta["generation_id"] == fixtures["complete"]["meta"]["generation_id"]
    assert calls == ["argo/data/meta.json"]
    assert "stale meta.json candidate" in caplog.text
    assert "age_seconds=960.000" in caplog.text
    assert "freshness_threshold_seconds=360.000" in caplog.text


def _stored_objects(publication, prefix="argo/data"):
    return {
        f"{prefix}/meta.json": json.dumps(publication.meta).encode(),
        f"{prefix}/workflows.parquet": publication.workflows,
        f"{prefix}/runs.parquet": publication.runs,
    }


def _stored_fixture_objects(case, prefix="argo/data"):
    """Materialize only the objects present in an incomplete fixture."""
    objects = {}
    present = set(case.get("present_objects", ()))
    if "meta.json" in present:
        objects[f"{prefix}/meta.json"] = json.dumps(case["meta"]).encode()
    if "workflows.parquet" in present:
        spec = case["workflows"]
        objects[f"{prefix}/workflows.parquet"] = parquet_io.table_to_parquet_bytes(
            spec["rows"], parquet_io.WORKFLOWS_SCHEMA, spec["generation_id"]
        )
    if "runs.parquet" in present:
        spec = case["runs"]
        objects[f"{prefix}/runs.parquet"] = parquet_io.table_to_parquet_bytes(
            spec["rows"], parquet_io.RUNS_SCHEMA, spec["generation_id"]
        )
    return objects


@pytest.mark.parametrize(
    "case_name", ["bootstrap_empty", "bootstrap_data_only", "bootstrap_meta_only"]
)
def test_read_generation_treats_bootstrap_object_sets_as_no_generation(
    fixtures, monkeypatch, case_name
):
    case = fixtures[case_name]
    objects = _stored_fixture_objects(case)
    calls = []

    def download(_s3, _bucket, key):
        calls.append(key)
        return objects.get(key)

    monkeypatch.setattr(consumer.s3io, "download_bytes", download)

    # There is no prior publication to retain. The incomplete object sets are
    # a normal pre-first-publication state, not an exception or an empty
    # snapshot.
    selected = consumer.read_generation(object(), "bucket", "argo/data")

    assert selected is None
    assert case["expected"]["selected"] is None
    assert calls == [f"argo/data/{name}" for name in case["expected"]["downloaded"]]


def test_read_generation_reads_meta_first_and_returns_a_zero_row_generation(
    fixtures, monkeypatch
):
    publication = _materialize(fixtures["zero_row"])
    objects = _stored_objects(publication)
    calls = []

    def download(_s3, _bucket, key):
        calls.append(key)
        return objects.get(key)

    monkeypatch.setattr(consumer.s3io, "download_bytes", download)

    selected = consumer.read_generation(object(), "bucket", "argo/data")

    assert selected is not None
    assert selected.meta["generation_id"] == publication.meta["generation_id"]
    assert selected.workflows == publication.workflows
    assert selected.runs == publication.runs
    assert calls == [
        "argo/data/meta.json",
        "argo/data/workflows.parquet",
        "argo/data/runs.parquet",
    ]
    assert consumer.is_complete_generation(selected)
    assert parquet_io.parquet_bytes_to_table(
        selected.workflows, parquet_io.WORKFLOWS_SCHEMA
    ).num_rows == 0


@pytest.mark.parametrize("missing", ["meta.json", "workflows.parquet", "runs.parquet"])
def test_read_generation_retains_the_last_complete_generation_when_an_object_is_missing(
    fixtures, monkeypatch, missing
):
    previous = _materialize(fixtures["complete"])
    objects = _stored_objects(previous)
    objects.pop(f"argo/data/{missing}")
    calls = []

    def download(_s3, _bucket, key):
        calls.append(key)
        return objects.get(key)

    monkeypatch.setattr(consumer.s3io, "download_bytes", download)

    selected = consumer.read_generation(object(), "bucket", "argo/data", previous)

    assert selected is previous
    assert consumer.is_complete_generation(selected)
    assert calls[0] == "argo/data/meta.json"
    if missing == "meta.json":
        assert calls == ["argo/data/meta.json"]
    else:
        assert calls == [
            "argo/data/meta.json",
            "argo/data/workflows.parquet",
            "argo/data/runs.parquet",
        ]


def test_read_generation_retains_the_last_complete_generation_for_a_torn_publication(
    fixtures, monkeypatch
):
    previous = _materialize(fixtures["complete"])
    torn = _materialize(fixtures["torn"])
    objects = _stored_objects(torn)

    monkeypatch.setattr(
        consumer.s3io,
        "download_bytes",
        lambda _s3, _bucket, key: objects.get(key),
    )

    selected = consumer.read_generation(object(), "bucket", "argo/data", previous)

    assert selected is previous
    assert selected.meta["generation_id"] == previous.meta["generation_id"]
    assert parquet_io.parquet_bytes_to_table(
        selected.workflows, parquet_io.WORKFLOWS_SCHEMA
    ).to_pylist()[0]["uid"] == "wf-complete"
