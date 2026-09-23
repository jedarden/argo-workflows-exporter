import io
import json

import pytest
from botocore.exceptions import ClientError

from src import main, parquet_io, workflows
from src.config import Cluster, Config, S3Endpoint


class _MemoryS3:
    def __init__(self, objects=None):
        self.objects = dict(objects or {})
        self.puts = 0

    def get_object(self, Bucket, Key):
        try:
            data = self.objects[Key]
        except KeyError:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "GetObject"
            ) from None
        return {"Body": io.BytesIO(data)}

    def put_object(self, Bucket, Key, Body, ContentType):
        self.puts += 1
        self._store(Bucket, Key, Body, ContentType)

    def _store(self, Bucket, Key, Body, ContentType):
        self.objects[Key] = Body


class _FailNthPut(_MemoryS3):
    """Fails the Nth put_object call with a 500, as the real client does once
    its retries are exhausted. Put order within a cycle is fixed:
    workflows.parquet, runs.parquet, meta.json."""

    def __init__(self, objects=None, fail_on_put=1):
        super().__init__(objects)
        self.fail_on_put = fail_on_put

    def put_object(self, Bucket, Key, Body, ContentType):
        # Counted even when it fails: puts is attempts, not successes.
        self.puts += 1
        if self.puts == self.fail_on_put:
            raise ClientError(
                {"Error": {"Code": "InternalError", "Message": "injected"}}, "PutObject"
            )
        self._store(Bucket, Key, Body, ContentType)


class _FailGet(_MemoryS3):
    """Fails get_object with a non-404 error: an outage, not a first run."""

    def get_object(self, Bucket, Key):
        raise ClientError(
            {"Error": {"Code": "InternalError", "Message": "injected"}}, "GetObject"
        )


def _config(clusters):
    return Config(
        clusters=clusters,
        namespace="",
        dest=S3Endpoint(
            endpoint_url="http://s3.example",
            access_key_id="access",
            secret_access_key="secret",
            bucket="bucket",
            addressing_style="path",
            region="us-east-1",
        ),
        dest_prefix="argo/data",
        version="test",
        poll_interval_seconds=300,
        run_retention_days=7,
        http_timeout_seconds=10,
        page_size=500,
        health_port=8080,
        log_level="INFO",
    )


def _workflow(uid, name):
    return {
        "metadata": {"uid": uid, "name": name, "namespace": "argo", "labels": {}},
        "spec": {},
        "status": {"phase": "Running"},
    }


def _list(monkeypatch, responses):
    monkeypatch.setattr(
        workflows,
        "list_items",
        lambda cluster, path, timeout, page_size: responses[cluster.name],
    )


def _seed_generation(objects, generated_at, gen_id, snapshot_rows, ledger_rows):
    """A complete, self-consistent generation, as a successful cycle leaves it."""
    objects["argo/data/workflows.parquet"] = parquet_io.table_to_parquet_bytes(
        snapshot_rows, parquet_io.WORKFLOWS_SCHEMA, gen_id
    )
    objects["argo/data/runs.parquet"] = parquet_io.table_to_parquet_bytes(
        ledger_rows, parquet_io.RUNS_SCHEMA, gen_id
    )
    objects["argo/data/meta.json"] = json.dumps(
        {
            "version": "test",
            "generated_at": generated_at,
            "generation_id": gen_id,
            "poll_interval_seconds": 300,
            "run_retention_days": 7,
            "clusters": [{"name": "ci", "ok": True, "workflows": len(snapshot_rows)}],
            "workflows": len(snapshot_rows),
            "runs": len(ledger_rows),
        }
    ).encode()
    return objects


def _paired(meta, workflows_bytes, runs_bytes):
    """The consumer-side pairing check from docs/notes/atomic-publication.md:
    all three objects of a readable generation carry the same generation id."""
    ids = {
        parquet_io.read_generation_id(workflows_bytes),
        parquet_io.read_generation_id(runs_bytes),
    }
    return ids == {meta["generation_id"]}


def test_mixed_reachability_omits_failed_cluster_and_its_prior_rows(monkeypatch):
    responses = {
        "ci": ([_workflow("ci-current", "ci-current")], True),
        "staging": ([], False),
    }
    monkeypatch.setattr(
        workflows,
        "list_items",
        lambda cluster, path, timeout, page_size: responses[cluster.name],
    )
    generated_at = main._now()
    monkeypatch.setattr(main, "_now", lambda: generated_at)
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

    assert main._run_cycle(
        _config([Cluster(name="ci"), Cluster(name="staging")]), s3
    ) is True

    snapshot = parquet_io.parquet_bytes_to_table(
        s3.objects["argo/data/workflows.parquet"], parquet_io.WORKFLOWS_SCHEMA
    )
    assert {(row["uid"], row["cluster"]) for row in snapshot.to_pylist()} == {
        ("ci-current", "ci")
    }
    meta = json.loads(s3.objects["argo/data/meta.json"])
    assert meta["generated_at"] == generated_at
    assert {
        stat["name"]: (stat["ok"], stat["workflows"])
        for stat in meta["clusters"]
    } == {"ci": (True, 1), "staging": (False, 0)}
    assert meta["workflows"] == 1


def test_partial_listing_omits_the_entire_cluster_from_the_new_snapshot(monkeypatch):
    responses = {
        "ci": ([_workflow("ci-current", "ci-current")], True),
        "staging": ([_workflow("staging-partial", "staging-partial")], False),
    }
    monkeypatch.setattr(
        workflows,
        "list_items",
        lambda cluster, path, timeout, page_size: responses[cluster.name],
    )
    generated_at = main._now()
    monkeypatch.setattr(main, "_now", lambda: generated_at)
    prior = [
        {
            "uid": "staging-prior",
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

    assert main._run_cycle(
        _config([Cluster(name="ci"), Cluster(name="staging")]), s3
    ) is True

    snapshot = parquet_io.parquet_bytes_to_table(
        s3.objects["argo/data/workflows.parquet"], parquet_io.WORKFLOWS_SCHEMA
    )
    assert {row["uid"] for row in snapshot.to_pylist()} == {"ci-current"}
    meta = json.loads(s3.objects["argo/data/meta.json"])
    assert {
        stat["name"]: (stat["ok"], stat["workflows"])
        for stat in meta["clusters"]
    } == {"ci": (True, 1), "staging": (False, 0)}
    assert meta["workflows"] == 1


_GENERATED_AT = "2026-09-23T19:00:00Z"
_OLD_GEN = f"{_GENERATED_AT}-000000aaaaaa"
_NEW_GENERATED_AT = "2026-09-23T19:01:00Z"


def _one_cluster_state(monkeypatch, s3):
    """A cycle listing one cluster with one workflow, publishing over `s3`."""
    _list(monkeypatch, {"ci": ([_workflow("wf-new", "wf-new")], True)})
    monkeypatch.setattr(main, "_now", lambda: _NEW_GENERATED_AT)
    return main._run_cycle(_config([Cluster(name="ci")]), s3)


def _prior_generation():
    """Stored objects as a previous successful cycle left them."""
    return _seed_generation(
        {},
        _GENERATED_AT,
        _OLD_GEN,
        [{"uid": "wf-old", "cluster": "ci", "observed_at": _GENERATED_AT}],
        [{"uid": "wf-old", "first_seen_at": _GENERATED_AT, "last_seen_at": _GENERATED_AT}],
    )


def test_one_generation_id_across_all_three_objects(monkeypatch):
    s3 = _MemoryS3()

    assert _one_cluster_state(monkeypatch, s3) is True

    meta = json.loads(s3.objects["argo/data/meta.json"])
    assert meta["generated_at"] == _NEW_GENERATED_AT
    assert meta["generation_id"].startswith(_NEW_GENERATED_AT)
    assert _paired(meta, s3.objects["argo/data/workflows.parquet"], s3.objects["argo/data/runs.parquet"])
    # File-level and row-level identities agree: the snapshot's rows were
    # observed by the very cycle the metadata names.
    snapshot = parquet_io.parquet_bytes_to_table(
        s3.objects["argo/data/workflows.parquet"], parquet_io.WORKFLOWS_SCHEMA
    )
    assert {row["observed_at"] for row in snapshot.to_pylist()} == {_NEW_GENERATED_AT}


def test_two_cycles_in_the_same_second_get_distinct_generation_ids(monkeypatch):
    """generated_at has second resolution and the poll interval is operator
    configured; the random suffix is what keeps two cycles that do land in
    the same second from being mistaken for one generation."""
    monkeypatch.setattr(main, "_now", lambda: _GENERATED_AT)
    s3 = _MemoryS3()
    cfg = _config([Cluster(name="ci")])
    _list(monkeypatch, {"ci": ([_workflow("wf-new", "wf-new")], True)})

    main._run_cycle(cfg, s3)
    first = json.loads(s3.objects["argo/data/meta.json"])["generation_id"]
    main._run_cycle(cfg, s3)
    second = json.loads(s3.objects["argo/data/meta.json"])["generation_id"]

    assert first != second
    assert parquet_io.read_generation_id(s3.objects["argo/data/runs.parquet"]) == second


def test_failed_workflows_upload_leaves_the_stored_generation_untouched(monkeypatch):
    s3 = _FailNthPut(_prior_generation(), fail_on_put=1)
    before = dict(s3.objects)

    with pytest.raises(ClientError):
        _one_cluster_state(monkeypatch, s3)

    assert s3.objects == before
    assert s3.puts == 1


def test_failed_runs_upload_leaves_a_detectably_torn_generation(monkeypatch):
    s3 = _FailNthPut(_prior_generation(), fail_on_put=2)

    with pytest.raises(ClientError):
        _one_cluster_state(monkeypatch, s3)

    meta = json.loads(s3.objects["argo/data/meta.json"])
    assert meta["generation_id"] == _OLD_GEN
    # The snapshot advanced, the ledger and its commit marker did not. The
    # consumer pairing check says torn, which is the only honest answer.
    assert parquet_io.read_generation_id(s3.objects["argo/data/workflows.parquet"]) != _OLD_GEN
    assert parquet_io.read_generation_id(s3.objects["argo/data/runs.parquet"]) == _OLD_GEN
    assert not _paired(meta, s3.objects["argo/data/workflows.parquet"], s3.objects["argo/data/runs.parquet"])


def test_failed_meta_upload_leaves_both_parquets_on_the_new_generation(monkeypatch):
    s3 = _FailNthPut(_prior_generation(), fail_on_put=3)
    meta_before = s3.objects["argo/data/meta.json"]

    with pytest.raises(ClientError):
        _one_cluster_state(monkeypatch, s3)

    # Both data objects carry the new generation; meta.json, the commit
    # marker, never landed and still names the old one.
    meta = json.loads(s3.objects["argo/data/meta.json"])
    assert s3.objects["argo/data/meta.json"] == meta_before
    assert meta["generation_id"] == _OLD_GEN
    new_id = parquet_io.read_generation_id(s3.objects["argo/data/workflows.parquet"])
    assert new_id.startswith(_NEW_GENERATED_AT)
    assert parquet_io.read_generation_id(s3.objects["argo/data/runs.parquet"]) == new_id
    assert not _paired(meta, s3.objects["argo/data/workflows.parquet"], s3.objects["argo/data/runs.parquet"])
    # The torn ledger is still this cycle's merge output: durable history
    # advanced even though the marker did not.
    runs = parquet_io.parquet_bytes_to_table(
        s3.objects["argo/data/runs.parquet"], parquet_io.RUNS_SCHEMA
    )
    assert {row["uid"] for row in runs.to_pylist()} == {"wf-old", "wf-new"}


def test_failed_ledger_download_writes_nothing_at_all(monkeypatch):
    s3 = _FailGet()

    with pytest.raises(ClientError):
        _one_cluster_state(monkeypatch, s3)

    assert s3.objects == {}
    assert s3.puts == 0


def test_next_successful_cycle_republishes_a_torn_generation_as_one(monkeypatch):
    """A torn publication is not repaired in place -- the whole cycle reruns
    and republishes all three objects under a fresh id, making the stored set
    readable again without operator action."""
    s3 = _FailNthPut(_prior_generation(), fail_on_put=3)
    with pytest.raises(ClientError):
        _one_cluster_state(monkeypatch, s3)
    torn_meta = json.loads(s3.objects["argo/data/meta.json"])
    assert not _paired(torn_meta, s3.objects["argo/data/workflows.parquet"], s3.objects["argo/data/runs.parquet"])

    healed = _MemoryS3(dict(s3.objects))
    _list(monkeypatch, {"ci": ([_workflow("wf-new", "wf-new")], True)})
    monkeypatch.setattr(main, "_now", lambda: "2026-09-23T19:02:00Z")
    assert main._run_cycle(_config([Cluster(name="ci")]), healed) is True

    meta = json.loads(healed.objects["argo/data/meta.json"])
    assert meta["generated_at"] == "2026-09-23T19:02:00Z"
    assert meta["generation_id"] != torn_meta["generation_id"]
    assert _paired(meta, healed.objects["argo/data/workflows.parquet"], healed.objects["argo/data/runs.parquet"])
    # The ledger kept both runs across the torn cycle: nothing was lost by
    # failing midway and republishing.
    runs = parquet_io.parquet_bytes_to_table(
        healed.objects["argo/data/runs.parquet"], parquet_io.RUNS_SCHEMA
    )
    assert {row["uid"] for row in runs.to_pylist()} == {"wf-old", "wf-new"}
