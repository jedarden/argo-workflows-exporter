"""Executable consumer fixtures for the generation-consistency contract.

The exporter publishes three independent objects. These tests materialize the
fixture rows as real Parquet bytes, then exercise the consumer's required
footer-only pairing check before any rows are interpreted. A torn candidate is
not mixed with the last complete generation.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

from botocore.exceptions import ClientError
import pyarrow as pa
import pytest

from src import consumer, meta_schema, parquet_io


FIXTURE_PATH = Path(__file__).with_name("fixtures") / "generation_consistency.json"
SCHEMA_EVOLUTION_FIXTURE_PATH = (
    Path(__file__).with_name("fixtures") / "parquet_schema_evolution.json"
)
TTL_BIAS_FIXTURE_PATH = Path(__file__).with_name("fixtures") / "ttl_bias.json"
_C_MAX_SECONDS = 60
_AT_STALE_BOUNDARY = datetime(2026, 9, 27, 11, 56, tzinfo=timezone.utc)

_FIXTURE_TYPES = {
    "string": pa.string(),
    "int32": pa.int32(),
    "int64": pa.int64(),
}


def _load_fixtures():
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _load_schema_evolution_fixtures():
    return json.loads(SCHEMA_EVOLUTION_FIXTURE_PATH.read_text(encoding="utf-8"))


def _load_ttl_bias_fixture():
    return json.loads(TTL_BIAS_FIXTURE_PATH.read_text(encoding="utf-8"))


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


def _fixture_schema(file_fixture):
    return pa.schema(
        [
            (field["name"], _FIXTURE_TYPES[field["type"]])
            for field in file_fixture["schema"]
        ]
    )


def _materialize_schema_evolution_file(release, output, fixtures):
    file_fixture = fixtures[release][output]
    schema = _fixture_schema(file_fixture)
    return parquet_io.table_to_parquet_bytes(
        file_fixture["rows"], schema, fixtures[release]["generation_id"]
    )


def _schema_evolution_publication(release, fixtures):
    return consumer.Publication(
        meta={"generation_id": fixtures[release]["generation_id"]},
        workflows=_materialize_schema_evolution_file(release, "workflows", fixtures),
        runs=_materialize_schema_evolution_file(release, "runs", fixtures),
    )


@pytest.fixture(scope="module")
def fixtures():
    return _load_fixtures()


@pytest.fixture(scope="module")
def schema_evolution_fixtures():
    return _load_schema_evolution_fixtures()


@pytest.fixture(scope="module")
def ttl_bias_fixture():
    return _load_ttl_bias_fixture()


@pytest.mark.parametrize("release", ["older", "newer"])
@pytest.mark.parametrize(
    ("output", "schema"),
    [("workflows", parquet_io.WORKFLOWS_SCHEMA), ("runs", parquet_io.RUNS_SCHEMA)],
    ids=["workflows", "runs"],
)
def test_consumer_normalizes_each_cross_release_file(
    schema_evolution_fixtures, release, output, schema
):
    """Each downloaded object is normalized before a consumer uses its rows."""
    stored = _materialize_schema_evolution_file(release, output, schema_evolution_fixtures)
    table = parquet_io.parquet_bytes_to_table(stored, schema)

    assert table.schema == schema
    assert table.column_names == schema.names
    assert all(
        table.column(name).type == schema.field(name).type for name in schema.names
    )
    assert "retired_column" not in table.column_names

    [row] = table.to_pylist()
    expected_uid = f"{'wf' if output == 'workflows' else 'run'}-{release}"
    assert row["uid"] == expected_uid
    assert row["duration_seconds"] == (42 if release == "older" else 84)

    if release == "older":
        # These fields did not exist in the older file. The null arrays must
        # still carry the current declared type so concatenation is safe.
        for name in ("failure_fingerprint", "failure_class"):
            assert table.column(name).type == pa.string()
            assert table.column(name).to_pylist() == [None]
    else:
        assert row["failure_fingerprint"]
        assert row["failure_class"]


def test_schema_normalization_does_not_depend_on_generation_pairing(
    schema_evolution_fixtures,
):
    """A torn pair is rejected, but each object's schema remains readable."""
    older = _schema_evolution_publication("older", schema_evolution_fixtures)
    newer = _schema_evolution_publication("newer", schema_evolution_fixtures)
    mixed = consumer.Publication(
        meta=older.meta,
        workflows=older.workflows,
        runs=newer.runs,
    )

    assert consumer.generation_ids(mixed) == {
        "meta": schema_evolution_fixtures["older"]["generation_id"],
        "workflows": schema_evolution_fixtures["older"]["generation_id"],
        "runs": schema_evolution_fixtures["newer"]["generation_id"],
    }
    assert consumer.is_complete_generation(mixed) is False
    assert consumer.select_generation(mixed, older) is older

    # Pairing is a publication-integrity check, not a prerequisite for schema
    # conformance. A reader can normalize either object independently while it
    # retains the last complete pair.
    old_workflows = parquet_io.parquet_bytes_to_table(
        mixed.workflows, parquet_io.WORKFLOWS_SCHEMA
    )
    new_runs = parquet_io.parquet_bytes_to_table(mixed.runs, parquet_io.RUNS_SCHEMA)
    assert old_workflows.schema == parquet_io.WORKFLOWS_SCHEMA
    assert new_runs.schema == parquet_io.RUNS_SCHEMA
    assert old_workflows.column("failure_class").to_pylist() == [None]
    assert new_runs.column("failure_class").to_pylist() == ["test_failure"]
    assert "retired_column" not in old_workflows.column_names
    assert "retired_column" not in new_runs.column_names


def test_ttl_biased_fixture_routes_historical_metrics_to_runs_not_workflows(
    ttl_bias_fixture,
):
    """Historical measures use the retained ledger, while live views use the snapshot.

    The fixture deliberately makes the two populations disagree. Completed
    successes have the shorter TTL, so the current listing is a biased sample
    of the retained run history. A consumer that accidentally counts
    ``workflows.parquet`` for a historical rate will therefore produce the
    snapshot's 50% result instead of the ledger's 90% result.
    """
    publication = _materialize(ttl_bias_fixture)

    assert consumer.is_complete_generation(publication)

    current_rows = parquet_io.parquet_bytes_to_table(
        publication.workflows, parquet_io.WORKFLOWS_SCHEMA
    ).to_pylist()
    historical_rows = parquet_io.parquet_bytes_to_table(
        publication.runs, parquet_io.RUNS_SCHEMA
    ).to_pylist()

    current_counts = {
        phase: sum(row["phase"] == phase for row in current_rows)
        for phase in ("Succeeded", "Failed")
    }
    historical_counts = {
        phase: sum(row["phase"] == phase for row in historical_rows)
        for phase in ("Succeeded", "Failed")
    }
    expected = ttl_bias_fixture["expected"]

    # Current-state views may use workflows.parquet: it contains only the
    # objects that survived their outcome-specific TTL windows.
    assert current_counts == expected["current_counts"]
    assert len(current_rows) == expected["current_total"]

    # Historical counts and rates must use runs.parquet, which retains rows
    # after the corresponding Workflow objects have been reaped.
    assert historical_counts == expected["historical_counts"]
    assert len(historical_rows) == expected["historical_total"]
    historical_success_rate = historical_counts["Succeeded"] / len(historical_rows)
    assert historical_success_rate == expected["historical_success_rate"]

    # Keep the populations observably different so this contract catches a
    # future regression that sources historical metrics from the live snapshot.
    current_success_rate = current_counts["Succeeded"] / len(current_rows)
    assert current_success_rate == expected["current_success_rate"]
    assert current_success_rate != historical_success_rate


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


@pytest.mark.parametrize(
    "case_name",
    ["complete", "zero_row", "recovered_cluster", "failed_clusters"],
)
def test_current_inventory_reads_workflows_snapshot_and_respects_availability(
    fixtures, case_name
):
    publication = _materialize(fixtures[case_name])
    expected = fixtures[case_name]["expected"]

    rows = consumer.current_snapshot_rows(publication)

    assert [row["uid"] for row in rows] == expected["workflow_uids"]
    assert {row["cluster"] for row in rows} == set(expected["workflow_clusters"])
    assert {
        row["cluster"]
        for row in rows
    } <= {
        name
        for name, available in expected["cluster_availability"].items()
        if available
    }

    if case_name == "zero_row":
        assert rows == []


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


def test_slow_cycle_freshness_ages_from_the_cycle_start_instant(fixtures):
    publication = _materialize(fixtures["complete"])
    meta = publication.meta
    finished_at = datetime(2026, 9, 27, 12, 4, tzinfo=timezone.utc)

    assert meta["generation_id"].startswith(f"{meta['generated_at']}-")
    assert consumer.freshness_age_seconds(meta, finished_at) == 240
    assert consumer.is_fresh(meta, finished_at, _C_MAX_SECONDS) is True


def test_consumer_rejects_generation_id_with_a_different_timestamp_prefix(fixtures):
    publication = _materialize(fixtures["complete"])
    mismatched_meta = dict(publication.meta)
    mismatched_meta["generated_at"] = "2026-09-27T12:01:00Z"
    mismatched = consumer.Publication(
        meta=mismatched_meta,
        workflows=publication.workflows,
        runs=publication.runs,
    )

    assert consumer.is_complete_generation(mismatched) is False
    assert consumer.select_generation(mismatched, publication) is publication


def _stored_objects(publication, prefix="argo/data"):
    return {
        f"{prefix}/meta.json": json.dumps(publication.meta).encode(),
        f"{prefix}/workflows.parquet": publication.workflows,
        f"{prefix}/runs.parquet": publication.runs,
    }


def _encoded_meta(meta, **changes):
    candidate = dict(meta)
    candidate.update(changes)
    return json.dumps(candidate).encode()


def _encoded_meta_without(meta, field):
    candidate = dict(meta)
    candidate.pop(field)
    return json.dumps(candidate).encode()


@pytest.mark.parametrize(
    "malformed_meta",
    [
        pytest.param(lambda meta: b'{"generated_at":', id="invalid-json"),
        pytest.param(
            lambda meta: _encoded_meta_without(meta, "version"),
            id="missing-version",
        ),
        pytest.param(
            lambda meta: _encoded_meta_without(meta, "generated_at"),
            id="missing-generated-at",
        ),
        pytest.param(
            lambda meta: _encoded_meta_without(meta, "generation_id"),
            id="missing-generation-id",
        ),
        pytest.param(
            lambda meta: _encoded_meta_without(meta, "poll_interval_seconds"),
            id="missing-poll-interval",
        ),
        pytest.param(
            lambda meta: _encoded_meta_without(meta, "clusters"),
            id="missing-clusters",
        ),
        pytest.param(
            lambda meta: _encoded_meta(meta, generated_at="2026-09-27 12:00:00"),
            id="generated-at-wrong-format",
        ),
        pytest.param(
            lambda meta: _encoded_meta(meta, generated_at="2026-02-30T12:00:00Z"),
            id="generated-at-impossible-date",
        ),
        pytest.param(
            lambda meta: _encoded_meta(meta, generated_at="2026-09-27T12:00:00+00:00"),
            id="generated-at-offset",
        ),
        pytest.param(
            lambda meta: _encoded_meta(meta, version=7),
            id="version-wrong-type",
        ),
        pytest.param(
            lambda meta: _encoded_meta(meta, poll_interval_seconds="60"),
            id="poll-interval-wrong-type",
        ),
        pytest.param(
            lambda meta: _encoded_meta(meta, clusters={"ci": True}),
            id="clusters-wrong-type",
        ),
        pytest.param(
            lambda meta: _encoded_meta(meta, generation_id="not-a-generation-id"),
            id="generation-id-wrong-shape",
        ),
        pytest.param(
            lambda meta: _encoded_meta(
                meta, generation_id="2026-09-27T12:00:00Z-ABCDEF123456"
            ),
            id="generation-id-uppercase-suffix",
        ),
        pytest.param(
            lambda meta: _encoded_meta(
                meta, generation_id="2026-09-27T12:01:00Z-abcdef123456"
            ),
            id="generation-id-timestamp-mismatch",
        ),
    ],
)
def test_read_generation_rejects_malformed_meta_and_holds_last_complete_generation(
    fixtures, monkeypatch, malformed_meta
):
    previous = _materialize(fixtures["complete"])
    objects = _stored_objects(previous)
    objects["argo/data/meta.json"] = malformed_meta(previous.meta)
    calls = []

    def download(_s3, _bucket, key):
        calls.append(key)
        return objects.get(key)

    monkeypatch.setattr(consumer.s3io, "download_bytes", download)

    selected = consumer.read_generation(
        object(),
        "bucket",
        "argo/data",
        previous,
        max_cycle_seconds=_C_MAX_SECONDS,
        now=datetime(2026, 9, 27, 12, 1, tzinfo=timezone.utc),
    )

    assert selected is previous
    assert calls == ["argo/data/meta.json"]


def test_read_generation_rejects_malformed_meta_before_first_generation(
    fixtures, monkeypatch
):
    valid = _materialize(fixtures["complete"])
    objects = _stored_objects(valid)
    objects["argo/data/meta.json"] = b"not-json"
    calls = []

    def download(_s3, _bucket, key):
        calls.append(key)
        return objects.get(key)

    monkeypatch.setattr(consumer.s3io, "download_bytes", download)

    selected = consumer.read_generation(
        object(),
        "bucket",
        "argo/data",
        max_cycle_seconds=_C_MAX_SECONDS,
        now=datetime(2026, 9, 27, 12, 1, tzinfo=timezone.utc),
    )

    assert selected is None
    assert calls == ["argo/data/meta.json"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("version", 7),
        ("generated_at", "2026-09-27 12:00:00"),
        ("generation_id", "not-a-generation-id"),
        ("poll_interval_seconds", "60"),
        ("run_retention_days", 0),
        ("clusters", [{"name": "ci", "ok": True, "workflows": "1"}]),
        ("clusters", [{"name": "ci", "ok": 1, "workflows": 1}]),
        ("clusters", [{"name": "ci", "ok": False, "workflows": 1}]),
        ("workflows", "1"),
        ("runs", True),
    ],
)
def test_direct_selection_rejects_malformed_meta(
    fixtures, field, value
):
    previous = _materialize(fixtures["complete"])
    malformed_meta = dict(previous.meta)
    malformed_meta[field] = value
    candidate = consumer.Publication(
        meta=malformed_meta,
        workflows=previous.workflows,
        runs=previous.runs,
    )

    assert consumer.is_complete_generation(candidate) is False
    assert consumer.select_generation(candidate, previous) is previous


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


@pytest.mark.parametrize(
    "object_name", ["meta.json", "workflows.parquet", "runs.parquet"]
)
@pytest.mark.parametrize(
    "retain_previous",
    [pytest.param(False, id="bootstrap"), pytest.param(True, id="last-complete")],
)
def test_read_generation_treats_each_missing_object_as_an_incomplete_publication(
    fixtures, monkeypatch, object_name, retain_previous
):
    candidate = _materialize(fixtures["complete"])
    previous = _materialize(fixtures["complete"]) if retain_previous else None
    objects = _stored_objects(candidate)
    objects.pop(f"argo/data/{object_name}")
    calls = []

    def download(_s3, _bucket, key):
        calls.append(key)
        return objects.get(key)

    monkeypatch.setattr(consumer.s3io, "download_bytes", download)

    selected = consumer.read_generation(object(), "bucket", "argo/data", previous)

    assert selected is previous
    assert calls == (
        ["argo/data/meta.json"]
        if object_name == "meta.json"
        else [
            "argo/data/meta.json",
            "argo/data/workflows.parquet",
            "argo/data/runs.parquet",
        ]
    )


@pytest.mark.parametrize("missing", ["workflows.parquet", "runs.parquet"])
@pytest.mark.parametrize(
    "retain_previous",
    [pytest.param(False, id="bootstrap"), pytest.param(True, id="last-complete")],
)
def test_read_generation_rejects_missing_parquet_without_empty_or_mixed_data(
    fixtures, monkeypatch, missing, retain_previous
):
    previous = _materialize(fixtures["complete"])
    candidate = _materialize(fixtures["recovered_cluster"])
    objects = _stored_objects(candidate)
    objects.pop(f"argo/data/{missing}")
    calls = []

    def download(_s3, _bucket, key):
        calls.append(key)
        return objects.get(key)

    monkeypatch.setattr(consumer.s3io, "download_bytes", download)

    selected = consumer.read_generation(
        object(),
        "bucket",
        "argo/data",
        previous if retain_previous else None,
    )

    # A valid marker does not make a missing data object an empty table. Both
    # data downloads must complete before the candidate can be considered.
    assert calls == [
        "argo/data/meta.json",
        "argo/data/workflows.parquet",
        "argo/data/runs.parquet",
    ]
    if retain_previous:
        # Identity and exact payload equality prove that the candidate's other
        # Parquet object was not mixed into the last complete generation.
        assert selected is previous
        assert selected.meta == previous.meta
        assert selected.workflows == previous.workflows
        assert selected.runs == previous.runs
        assert selected.meta["generation_id"] != candidate.meta["generation_id"]
    else:
        assert selected is None


@pytest.mark.parametrize(
    "object_name", ["meta.json", "workflows.parquet", "runs.parquet"]
)
@pytest.mark.parametrize(
    "retain_previous",
    [pytest.param(False, id="bootstrap"), pytest.param(True, id="last-complete")],
)
@pytest.mark.parametrize(
    "error_code", [pytest.param("AccessDenied"), pytest.param("InternalError")]
)
def test_read_generation_propagates_non_missing_s3_errors_from_every_object(
    fixtures, monkeypatch, object_name, retain_previous, error_code
):
    candidate = _materialize(fixtures["complete"])
    previous = _materialize(fixtures["complete"]) if retain_previous else None
    objects = _stored_objects(candidate)
    failing_key = f"argo/data/{object_name}"
    error = ClientError(
        {"Error": {"Code": error_code, "Message": "injected storage failure"}},
        "GetObject",
    )
    calls = []

    def download(_s3, _bucket, key):
        calls.append(key)
        if key == failing_key:
            raise error
        return objects[key]

    monkeypatch.setattr(consumer.s3io, "download_bytes", download)

    with pytest.raises(ClientError) as raised:
        consumer.read_generation(object(), "bucket", "argo/data", previous)

    assert raised.value is error
    assert calls == (
        ["argo/data/meta.json"]
        if object_name == "meta.json"
        else [
            "argo/data/meta.json",
            "argo/data/workflows.parquet",
            *(["argo/data/runs.parquet"] if object_name == "runs.parquet" else []),
        ]
    )


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


@pytest.mark.parametrize("corruption", ["truncated", "non-parquet"])
@pytest.mark.parametrize("object_name", ["workflows.parquet", "runs.parquet"])
def test_read_generation_rejects_unreadable_newer_objects_and_retains_previous(
    fixtures, monkeypatch, caplog, corruption, object_name
):
    previous = _materialize(fixtures["complete"])
    newer = _materialize(fixtures["recovered_cluster"])
    objects = _stored_objects(newer)
    key = f"argo/data/{object_name}"
    if corruption == "truncated":
        objects[key] = objects[key][:-8]
    else:
        objects[key] = b"not a parquet object"
    calls = []

    def download(_s3, _bucket, requested_key):
        calls.append(requested_key)
        return objects.get(requested_key)

    monkeypatch.setattr(consumer.s3io, "download_bytes", download)
    caplog.set_level("WARNING", logger="src.consumer")

    selected = consumer.read_generation(object(), "bucket", "argo/data", previous)

    assert selected is previous
    assert calls == [
        "argo/data/meta.json",
        "argo/data/workflows.parquet",
        "argo/data/runs.parquet",
    ]
    assert "could not read a Parquet generation footer" in caplog.text
    assert selected.workflows == previous.workflows
    assert selected.runs == previous.runs


def test_read_generation_distinguishes_corrupt_data_from_absent_bootstrap(
    fixtures, monkeypatch, caplog
):
    candidate = _materialize(fixtures["complete"])
    objects = _stored_objects(candidate)
    objects["argo/data/workflows.parquet"] = b"not a parquet object"
    calls = []

    def download(_s3, _bucket, key):
        calls.append(key)
        return objects.get(key)

    monkeypatch.setattr(consumer.s3io, "download_bytes", download)
    caplog.set_level("WARNING", logger="src.consumer")

    selected = consumer.read_generation(object(), "bucket", "argo/data")

    assert selected is None
    assert calls == [
        "argo/data/meta.json",
        "argo/data/workflows.parquet",
        "argo/data/runs.parquet",
    ]
    assert "could not read a Parquet generation footer" in caplog.text


def test_overlapping_writers_never_expose_an_interleaved_generation(
    fixtures, monkeypatch, caplog
):
    """An interleaved publication is held until one writer is complete.

    Writer A and writer B each publish the three objects in the documented
    order, but their PUTs overlap.  The consumer must retain the last complete
    generation for every intermediate object set, including the case where A
    advances the marker after B's data objects have landed.
    """
    previous = _materialize(fixtures["complete"])
    writer_a = _materialize(fixtures["recovered_cluster"])
    writer_b = _materialize(fixtures["zero_row"])
    objects = _stored_objects(previous)

    def download(_s3, _bucket, key):
        return objects.get(key)

    monkeypatch.setattr(consumer.s3io, "download_bytes", download)
    caplog.set_level("WARNING", logger="src.consumer")

    updates = [
        ("workflows.parquet", writer_a.workflows),
        ("workflows.parquet", writer_b.workflows),
        ("runs.parquet", writer_a.runs),
        ("runs.parquet", writer_b.runs),
        # A's marker now describes B's data objects: a mixed generation.
        ("meta.json", json.dumps(writer_a.meta).encode()),
    ]
    for name, payload in updates:
        objects[f"argo/data/{name}"] = payload
        assert consumer.read_generation(
            object(), "bucket", "argo/data", previous
        ) is previous

    assert "rejecting inconsistent publication generation ids" in caplog.text

    # Once B's marker lands, all three objects agree and only then may the
    # consumer advance.  No rows from A leaked into the selected publication.
    objects["argo/data/meta.json"] = json.dumps(writer_b.meta).encode()
    selected = consumer.read_generation(object(), "bucket", "argo/data", previous)
    assert selected is not None
    assert selected.meta["generation_id"] == writer_b.meta["generation_id"]
    assert [row["uid"] for row in parquet_io.parquet_bytes_to_table(
        selected.workflows, parquet_io.WORKFLOWS_SCHEMA
    ).to_pylist()] == []
    assert [row["uid"] for row in parquet_io.parquet_bytes_to_table(
        selected.runs, parquet_io.RUNS_SCHEMA
    ).to_pylist()] == []
