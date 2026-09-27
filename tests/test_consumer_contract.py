"""Executable consumer fixtures for the generation-consistency contract.

The exporter publishes three independent objects. These tests materialize the
fixture rows as real Parquet bytes, then exercise the consumer's required
footer-only pairing check before any rows are interpreted. A torn candidate is
not mixed with the last complete generation.
"""

import json
from pathlib import Path

import pytest

from src import meta_schema, parquet_io


FIXTURE_PATH = Path(__file__).with_name("fixtures") / "generation_consistency.json"


def _load_fixtures():
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _materialize(case):
    """Build the three downloaded objects represented by one fixture case."""
    return {
        "meta": case["meta"],
        "workflows": parquet_io.table_to_parquet_bytes(
            case["workflows"]["rows"],
            parquet_io.WORKFLOWS_SCHEMA,
            case["workflows"]["generation_id"],
        ),
        "runs": parquet_io.table_to_parquet_bytes(
            case["runs"]["rows"],
            parquet_io.RUNS_SCHEMA,
            case["runs"]["generation_id"],
        ),
    }


def _footer_generation_ids(publication):
    """Read only the Parquet footers needed for the pairing decision."""
    return {
        "meta": publication["meta"]["generation_id"],
        "workflows": parquet_io.read_generation_id(publication["workflows"]),
        "runs": parquet_io.read_generation_id(publication["runs"]),
    }


def _is_complete_generation(publication):
    ids = _footer_generation_ids(publication)
    return ids["meta"] is not None and len(set(ids.values())) == 1


def _select_generation(candidate, last_complete):
    """The minimum consumer policy: reject a torn candidate, retain prior data."""
    return candidate if _is_complete_generation(candidate) else last_complete


@pytest.fixture(scope="module")
def fixtures():
    return _load_fixtures()


@pytest.mark.parametrize(
    "case_name", ["complete", "torn", "zero_row", "failed_clusters"]
)
def test_fixture_sidecars_and_both_parquet_footers_are_executable(fixtures, case_name):
    case = fixtures[case_name]
    publication = _materialize(case)

    meta_schema.validate(publication["meta"])
    ids = _footer_generation_ids(publication)
    assert ids["workflows"] == case["workflows"]["generation_id"]
    assert ids["runs"] == case["runs"]["generation_id"]
    assert _is_complete_generation(publication) is case["expected"]["accepted"]


def test_torn_fixture_retains_the_last_complete_generation(fixtures):
    complete = _materialize(fixtures["complete"])
    torn = _materialize(fixtures["torn"])

    assert _footer_generation_ids(torn) == {
        "meta": "2026-09-27T12:00:00Z-111111aaaaaa",
        "workflows": "2026-09-27T12:01:00Z-222222bbbbbb",
        "runs": "2026-09-27T12:00:00Z-111111aaaaaa",
    }
    selected = _select_generation(torn, complete)
    assert selected is complete
    assert selected["meta"]["generation_id"] == fixtures["complete"]["meta"]["generation_id"]

    # The fallback is what a consumer presents; it must not leak the torn
    # snapshot's rows into a generation whose marker still names the old one.
    selected_rows = parquet_io.parquet_bytes_to_table(
        selected["workflows"], parquet_io.WORKFLOWS_SCHEMA
    ).to_pylist()
    assert [row["uid"] for row in selected_rows] == ["wf-complete"]


@pytest.mark.parametrize("case_name", ["zero_row", "failed_clusters"])
def test_valid_non_torn_fixtures_are_selected_and_preserve_snapshot_semantics(
    fixtures, case_name
):
    case = fixtures[case_name]
    publication = _materialize(case)
    selected = _select_generation(publication, _materialize(fixtures["complete"]))

    assert selected is publication
    workflows = parquet_io.parquet_bytes_to_table(
        selected["workflows"], parquet_io.WORKFLOWS_SCHEMA
    ).to_pylist()
    runs = parquet_io.parquet_bytes_to_table(
        selected["runs"], parquet_io.RUNS_SCHEMA
    ).to_pylist()
    expected = case["expected"]
    assert [row["uid"] for row in workflows] == expected["workflow_uids"]
    assert [row["uid"] for row in runs] == expected["run_uids"]
    assert {
        cluster["name"]
        for cluster in case["meta"]["clusters"]
        if not cluster["ok"]
    } == set(expected["unavailable_clusters"])

    if case_name == "zero_row":
        assert workflows == []
        assert runs == []
    else:
        # Failed-cluster history remains in runs.parquet, but unavailable
        # clusters are absent from this generation's current snapshot.
        assert {row["cluster"] for row in workflows} == {"ci"}
        assert {row["cluster"] for row in runs} == {"ci", "staging"}
