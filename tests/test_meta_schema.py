"""The meta.json contract: what validate() accepts, and what the publisher
actually writes in the cycles whose semantics the docs promise -- mixed
success (some clusters answering, some not) and no success at all."""

import copy
import json

import pytest

from src import main, meta_schema, parquet_io
from src.config import Cluster
from tests.test_main import (
    _MemoryS3,
    _config,
    _list,
    _seed_generation,
    _workflow,
)

_GENERATED_AT = "2026-09-23T19:00:00Z"

# The documented example shape, as _run_cycle builds it.
_VALID = {
    "version": "0.2.3",
    "generated_at": _GENERATED_AT,
    "generation_id": "2026-09-23T19:00:00Z-3f9c2a1b7d44",
    "poll_interval_seconds": 300,
    "run_retention_days": 7,
    "clusters": [{"name": "ci", "ok": True, "workflows": 64}],
    "workflows": 64,
    "runs": 812,
}


def _cycle(monkeypatch, responses, s3, generated_at=_GENERATED_AT):
    """Publishes one cycle over `s3` with every cluster in `responses`
    (name -> (items, complete))."""
    _list(monkeypatch, responses)
    monkeypatch.setattr(main, "_now", lambda: generated_at)
    return main._run_cycle(_config([Cluster(name=name) for name in responses]), s3)


# -- what validate() accepts --


def test_canonical_sidecar_validates():
    meta_schema.validate(copy.deepcopy(_VALID))


def test_empty_but_reachable_cluster_is_valid():
    """ok=true with workflows=0 is a confirmed empty listing -- the sidecar
    must accept it, and must not confuse it with the ok=false outage shape."""
    meta = copy.deepcopy(_VALID)
    meta["clusters"] = [{"name": "ci", "ok": True, "workflows": 0}]
    meta["workflows"] = 0
    meta_schema.validate(meta)


# -- per-field structure --


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("generated_at", "2026-09-23 19:00:00"),  # not the published format
        ("generated_at", "2026-09-23T19:00:00+00:00"),  # an offset, not Z
        ("generated_at", "2026-09-23T19:00:00.500Z"),  # sub-second
        ("generated_at", "2026-02-30T19:00:00Z"),  # shaped right, not a date
        ("generated_at", None),
        ("generation_id", "2026-09-23T19:00:00Z"),  # no suffix at all
        ("generation_id", "2026-09-23T19:00:00Z-short"),
        ("generation_id", "2026-09-23T19:00:00Z-3F9C2A1B7D44"),  # uppercase hex
        ("poll_interval_seconds", 0),
        ("poll_interval_seconds", 300.0),
        ("poll_interval_seconds", True),  # bool is not an integer here
        ("run_retention_days", -1),
        ("version", ""),
        ("version", None),
    ],
)
def test_bad_field_values_are_rejected(field, value):
    meta = dict(_VALID, **{field: value})
    with pytest.raises(meta_schema.MetaSchemaError, match=field):
        meta_schema.validate(meta)


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda m: m.pop("runs"), id="missing runs"),
        pytest.param(lambda m: m.pop("clusters"), id="missing clusters"),
        pytest.param(lambda m: m.update(extra=True), id="unexpected top-level field"),
        pytest.param(lambda m: m.update(workflows="64"), id="workflows as string"),
        pytest.param(lambda m: m.update(workflows=-1), id="negative workflows"),
        pytest.param(lambda m: m.update(clusters=[]), id="empty clusters array"),
        pytest.param(lambda m: m.update(runs=True), id="runs as bool"),
        pytest.param(lambda m: m["clusters"][0].pop("ok"), id="cluster missing ok"),
        pytest.param(lambda m: m["clusters"][0].pop("workflows"), id="cluster missing count"),
        pytest.param(lambda m: m["clusters"][0].update(ok=1), id="ok as 1"),
        pytest.param(lambda m: m["clusters"][0].update(ok="yes"), id="ok as string"),
        pytest.param(lambda m: m["clusters"][0].update(workflows=-1), id="negative cluster count"),
        pytest.param(lambda m: m["clusters"][0].update(name=""), id="empty cluster name"),
        pytest.param(lambda m: m["clusters"][0].update(note="x"), id="unexpected cluster field"),
        pytest.param(
            lambda m: m["clusters"].append(dict(m["clusters"][0])), id="duplicate cluster"
        ),
    ],
)
def test_bad_shapes_are_rejected(mutate):
    meta = copy.deepcopy(_VALID)
    mutate(meta)
    with pytest.raises(meta_schema.MetaSchemaError):
        meta_schema.validate(meta)


# -- cross-field rules --


def test_generation_id_must_extend_generated_at():
    meta = copy.deepcopy(_VALID)
    # Well-formed on its own -- but it names another second than the
    # generated_at it is published beside.
    meta["generation_id"] = "2026-09-23T19:01:00Z-aaaaaaaaaaaa"
    with pytest.raises(meta_schema.MetaSchemaError, match="extend meta.generated_at"):
        meta_schema.validate(meta)


def test_top_level_workflows_must_sum_the_ok_clusters():
    meta = copy.deepcopy(_VALID)
    meta["workflows"] = 63
    with pytest.raises(meta_schema.MetaSchemaError, match="sum of the ok clusters"):
        meta_schema.validate(meta)


def test_unavailable_cluster_must_report_zero():
    meta = copy.deepcopy(_VALID)
    meta["clusters"].append({"name": "staging", "ok": False, "workflows": 5})
    # This payload breaks the sum rule too; the contract reports both, and
    # the zero rule is the one about what an unavailable cluster may claim.
    with pytest.raises(meta_schema.MetaSchemaError, match="must report\\s+workflows=0"):
        meta_schema.validate(meta)


# -- what the publisher writes --


def test_first_cycle_publishes_a_schema_valid_sidecar(monkeypatch):
    s3 = _MemoryS3()

    assert _cycle(monkeypatch, {"ci": ([_workflow("u1", "n1")], True)}, s3) is True

    meta = json.loads(s3.objects["argo/data/meta.json"])
    meta_schema.validate(meta)
    assert meta["generated_at"] == _GENERATED_AT
    assert meta["generation_id"].startswith(_GENERATED_AT)


def test_mixed_success_cycle_publishes_a_schema_valid_sidecar(monkeypatch):
    """One cluster answers, one fails: the sidecar stays valid and every
    count says exactly what the docs promise -- the failed cluster reports
    zero and contributes no rows to the top-level count."""
    prior = [
        {
            "uid": "staging-old",
            "cluster": "staging",
            "observed_at": "2026-09-22T00:00:00Z",
        }
    ]
    s3 = _MemoryS3(
        {
            "argo/data/workflows.parquet": parquet_io.table_to_parquet_bytes(
                prior, parquet_io.WORKFLOWS_SCHEMA
            )
        }
    )
    responses = {
        "ci": ([_workflow("ci-1", "ci-1"), _workflow("ci-2", "ci-2")], True),
        "staging": ([_workflow("staging-partial", "staging-partial")], False),
    }

    assert _cycle(monkeypatch, responses, s3) is True

    meta = json.loads(s3.objects["argo/data/meta.json"])
    meta_schema.validate(meta)
    assert {
        stat["name"]: (stat["ok"], stat["workflows"]) for stat in meta["clusters"]
    } == {"ci": (True, 2), "staging": (False, 0)}
    # Only the answered cluster's rows exist, and only they are counted.
    assert meta["workflows"] == 2


def test_confirmed_empty_cluster_is_ok_true_with_zero(monkeypatch):
    """The shape the outage must not be mistaken for: a reachable cluster
    that genuinely lists zero workflows."""
    responses = {
        "ci": ([], True),
        "staging": ([_workflow("staging-1", "staging-1")], True),
    }
    s3 = _MemoryS3()

    assert _cycle(monkeypatch, responses, s3) is True

    meta = json.loads(s3.objects["argo/data/meta.json"])
    meta_schema.validate(meta)
    assert {
        stat["name"]: (stat["ok"], stat["workflows"]) for stat in meta["clusters"]
    } == {"ci": (True, 0), "staging": (True, 1)}
    assert meta["workflows"] == 1


def test_no_success_cycle_leaves_the_last_valid_generation_untouched(monkeypatch):
    """Every cluster down: the cycle writes nothing, so the stored sidecar
    stays exactly as the last successful cycle left it -- still valid, with
    a generated_at that now reads as stale. That staleness is the outage
    signal, which is why no fresh sidecar may be stamped over it."""
    s3 = _MemoryS3()
    _seed_generation(
        s3.objects,
        "2026-09-22T00:00:00Z",
        "2026-09-22T00:00:00Z-000000aaaaaa",
        [{"uid": "wf-old", "cluster": "ci", "observed_at": "2026-09-22T00:00:00Z"}],
        [{"uid": "wf-old", "first_seen_at": "2026-09-22T00:00:00Z", "last_seen_at": "2026-09-22T00:00:00Z"}],
    )
    before = dict(s3.objects)
    responses = {"ci": ([_workflow("ci-1", "ci-1")], False), "staging": ([], False)}

    assert _cycle(monkeypatch, responses, s3) is False

    assert s3.objects == before
    meta = json.loads(s3.objects["argo/data/meta.json"])
    meta_schema.validate(meta)
    assert meta["generated_at"] == "2026-09-22T00:00:00Z"


def test_no_success_cycle_on_a_fresh_store_writes_nothing_at_all(monkeypatch):
    s3 = _MemoryS3()

    assert _cycle(monkeypatch, {"ci": ([], False)}, s3) is False

    assert s3.objects == {}


def test_a_contract_violation_publishes_nothing(monkeypatch):
    """The publisher validates its sidecar before the first upload, so a
    writer regression refuses the cycle instead of shipping a commit marker
    that misdescribes the generation beside it."""
    s3 = _MemoryS3()
    _list(monkeypatch, {"ci": ([_workflow("ci-1", "ci-1")], True)})
    monkeypatch.setattr(main, "_now", lambda: _GENERATED_AT)

    def refuse(_meta):
        raise meta_schema.MetaSchemaError("injected")

    monkeypatch.setattr(main.meta_schema, "validate", refuse)
    with pytest.raises(meta_schema.MetaSchemaError, match="injected"):
        main._run_cycle(_config([Cluster(name="ci")]), s3)

    assert s3.objects == {}
