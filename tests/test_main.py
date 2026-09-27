import http.client
import io
import json
import logging
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
import pyarrow as pa
from botocore.exceptions import ClientError, ReadTimeoutError

from src import k8s_api, ledger, main, meta_schema, parquet_io, s3io, workflows
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
        self.states_after_successful_put = []

    def put_object(self, Bucket, Key, Body, ContentType):
        # Counted even when it fails: puts is attempts, not successes.
        self.puts += 1
        if self.puts == self.fail_on_put:
            raise ClientError(
                {"Error": {"Code": "InternalError", "Message": "injected"}}, "PutObject"
            )
        self._store(Bucket, Key, Body, ContentType)
        self.states_after_successful_put.append(dict(self.objects))


class _FailGet(_MemoryS3):
    """Fails get_object with an injected error, not a first-run miss."""

    def __init__(self, objects=None, error=None):
        super().__init__(objects)
        self.error = error or ClientError(
            {"Error": {"Code": "InternalError", "Message": "injected"}}, "GetObject"
        )

    def get_object(self, Bucket, Key):
        raise self.error


class _RecordingS3(_MemoryS3):
    """Records object order while retaining the in-memory S3 behavior."""

    def __init__(self, objects=None):
        super().__init__(objects)
        self.uploaded_keys = []

    def put_object(self, Bucket, Key, Body, ContentType):
        self.uploaded_keys.append(Key)
        super().put_object(Bucket, Key, Body, ContentType)


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


def _workflow(uid, name, phase="Running", **status):
    return {
        "metadata": {"uid": uid, "name": name, "namespace": "argo", "labels": {}},
        "spec": {},
        "status": {"phase": phase, **status},
    }


def _list(monkeypatch, responses):
    monkeypatch.setattr(
        workflows,
        "list_items",
        lambda cluster, path, timeout, page_size: responses[cluster.name],
    )


def test_restart_rehydrates_ledger_and_tracks_running_to_terminal_state(monkeypatch):
    cfg = _config([Cluster(name="ci")])
    monkeypatch.setattr(ledger, "_cutoff", lambda retention_days: "2026-09-16T12:00:00Z")

    monkeypatch.setattr(main, "_now", lambda: "2026-09-23T12:00:00Z")
    _list(
        monkeypatch,
        {
            "ci": (
                [
                    _workflow(
                        "wf-1",
                        "build-1",
                        startedAt="2026-09-23T11:00:00Z",
                    )
                ],
                True,
            )
        },
    )
    first_process = _MemoryS3()

    assert main._run_cycle(cfg, first_process) is True

    first_runs = parquet_io.parquet_bytes_to_table(
        first_process.objects["argo/data/runs.parquet"], parquet_io.RUNS_SCHEMA
    ).to_pylist()
    assert [(row["uid"], row["phase"], row["duration_seconds"]) for row in first_runs] == [
        ("wf-1", "Running", None)
    ]

    restarted_process = _MemoryS3(first_process.objects)
    monkeypatch.setattr(main, "_now", lambda: "2026-09-23T12:05:00Z")
    _list(
        monkeypatch,
        {
            "ci": (
                [
                    _workflow(
                        "wf-1",
                        "build-1",
                        phase="Succeeded",
                        startedAt="2026-09-23T11:00:00Z",
                        finishedAt="2026-09-23T11:01:02Z",
                        message="completed",
                    )
                ],
                True,
            )
        },
    )

    assert main._run_cycle(cfg, restarted_process) is True

    runs = parquet_io.parquet_bytes_to_table(
        restarted_process.objects["argo/data/runs.parquet"], parquet_io.RUNS_SCHEMA
    ).to_pylist()
    assert len(runs) == 1
    assert {
        key: runs[0][key]
        for key in (
            "uid",
            "phase",
            "message",
            "finished_at",
            "duration_seconds",
            "first_seen_at",
            "last_seen_at",
        )
    } == {
        "uid": "wf-1",
        "phase": "Succeeded",
        "message": "completed",
        "finished_at": "2026-09-23T11:01:02Z",
        "duration_seconds": 62,
        "first_seen_at": "2026-09-23T12:00:00Z",
        "last_seen_at": "2026-09-23T12:05:00Z",
    }


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


def _consumer_generation(objects, fallback_generation):
    """Return the generation a pairing-aware consumer is allowed to expose."""
    meta = json.loads(objects["argo/data/meta.json"])
    if _paired(
        meta,
        objects["argo/data/workflows.parquet"],
        objects["argo/data/runs.parquet"],
    ):
        return meta["generation_id"]
    return fallback_generation


def test_successful_multi_cluster_cycle_publishes_one_readable_generation(monkeypatch):
    cases = json.loads(
        (Path(__file__).with_name("fixtures") / "workflow_cases.json").read_text(
            encoding="utf-8"
        )
    )
    generated_at = "2026-09-23T19:00:00Z"
    cfg = replace(
        _config(
            [
                Cluster(name="local"),
                Cluster(
                    name="remote",
                    base_url="http://proxy.example:8001",
                    namespace="argo-prod",
                ),
            ]
        ),
        namespace="argo",
        page_size=1,
        http_timeout_seconds=17,
    )
    monkeypatch.setattr(main, "_now", lambda: generated_at)
    monkeypatch.setattr(ledger, "_cutoff", lambda _retention_days: "2026-09-16T19:00:00Z")

    local_calls = []
    local_pages = {
        None: {
            "items": [cases["namespaced_template"]],
            "metadata": {"continue": "local-next"},
        },
        "local-next": {
            "items": [cases["compressed_nodes"]],
            "metadata": {},
        },
    }

    def fake_local_request(path, params, timeout):
        local_calls.append((path, dict(params), timeout))
        return SimpleNamespace(
            status_code=200,
            json=lambda: local_pages[params.get("continue")],
        )

    remote_calls = []
    remote_pages = {
        None: {
            "items": [cases["event_precedence"]],
            "metadata": {"continue": "remote-next"},
        },
        "remote-next": {
            "items": [cases["completed_workflow"]],
            "metadata": {},
        },
    }

    def fake_get(url, params=None, **kwargs):
        params = dict(params or {})
        remote_calls.append((url, params, kwargs))
        return SimpleNamespace(
            status_code=200,
            json=lambda: remote_pages[params.get("continue")],
        )

    monkeypatch.setattr(k8s_api, "_local_request", fake_local_request)
    monkeypatch.setattr(k8s_api.requests, "get", fake_get)

    prior_runs = [
        {
            "uid": "uid-completed",
            "cluster": "remote",
            "phase": "Running",
            "first_seen_at": "2026-09-22T18:00:00Z",
            "last_seen_at": "2026-09-22T19:00:00Z",
        },
        {
            "uid": "uid-reaped",
            "cluster": "local",
            "phase": "Succeeded",
            "first_seen_at": "2026-09-22T20:00:00Z",
            "last_seen_at": "2026-09-22T21:00:00Z",
        },
    ]

    class RecordingS3(_MemoryS3):
        def __init__(self, objects=None):
            super().__init__(objects)
            self.uploads = []

        def put_object(self, Bucket, Key, Body, ContentType):
            self.uploads.append((Bucket, Key, ContentType))
            super().put_object(Bucket, Key, Body, ContentType)

    s3 = RecordingS3(
        {
            "argo/data/runs.parquet": parquet_io.table_to_parquet_bytes(
                prior_runs, parquet_io.RUNS_SCHEMA
            )
        }
    )

    assert main._run_cycle(cfg, s3) is True
    assert local_calls == [
        (
            "/apis/argoproj.io/v1alpha1/namespaces/argo/workflows",
            {"limit": 1},
            17,
        ),
        (
            "/apis/argoproj.io/v1alpha1/namespaces/argo/workflows",
            {"limit": 1, "continue": "local-next"},
            17,
        ),
    ]
    assert remote_calls == [
        (
            "http://proxy.example:8001/apis/argoproj.io/v1alpha1/namespaces/argo-prod/workflows",
            {"limit": 1},
            {"timeout": 17},
        ),
        (
            "http://proxy.example:8001/apis/argoproj.io/v1alpha1/namespaces/argo-prod/workflows",
            {"limit": 1, "continue": "remote-next"},
            {"timeout": 17},
        ),
    ]
    assert all("watch" not in params for _, params, _ in local_calls)
    assert all("watch" not in params for _, params, _ in remote_calls)
    assert s3.uploads == [
        ("bucket", "argo/data/workflows.parquet", "application/octet-stream"),
        ("bucket", "argo/data/runs.parquet", "application/octet-stream"),
        ("bucket", "argo/data/meta.json", "application/json"),
    ]

    workflows_bytes = s3io.download_bytes(s3, "bucket", "argo/data/workflows.parquet")
    runs_bytes = s3io.download_bytes(s3, "bucket", "argo/data/runs.parquet")
    meta = json.loads(s3io.download_bytes(s3, "bucket", "argo/data/meta.json"))
    meta_schema.validate(meta)
    assert meta["generated_at"] == generated_at
    assert meta["generation_id"].startswith(f"{generated_at}-")
    assert _paired(meta, workflows_bytes, runs_bytes)
    assert {
        (stat["name"], stat["ok"], stat["workflows"]) for stat in meta["clusters"]
    } == {("local", True, 2), ("remote", True, 2)}
    assert meta["workflows"] == 4
    assert meta["runs"] == 5

    snapshot_table = parquet_io.parquet_bytes_to_table(
        workflows_bytes, parquet_io.WORKFLOWS_SCHEMA
    )
    runs_table = parquet_io.parquet_bytes_to_table(runs_bytes, parquet_io.RUNS_SCHEMA)
    assert snapshot_table.schema == parquet_io.WORKFLOWS_SCHEMA
    assert runs_table.schema == parquet_io.RUNS_SCHEMA
    snapshot = snapshot_table.to_pylist()
    runs = runs_table.to_pylist()
    assert len(snapshot) == meta["workflows"]
    assert len(runs) == meta["runs"]
    assert sum(
        stat["workflows"] for stat in meta["clusters"] if stat["ok"]
    ) == meta["workflows"]

    snapshot_by_key = {(row["cluster"], row["uid"]): row for row in snapshot}
    assert set(snapshot_by_key) == {
        ("local", "uid-namespaced-template"),
        ("local", "uid-compressed-nodes"),
        ("remote", "uid-event"),
        ("remote", "uid-completed"),
    }
    assert {row["observed_at"] for row in snapshot} == {generated_at}
    assert snapshot_by_key["local", "uid-namespaced-template"]["template"] == "example-build"
    assert snapshot_by_key["local", "uid-namespaced-template"]["template_scope"] == "namespaced"
    assert snapshot_by_key["local", "uid-compressed-nodes"]["failed_step"] is None
    assert snapshot_by_key["local", "uid-compressed-nodes"]["failure_class"] == "timeout"
    assert snapshot_by_key["local", "uid-compressed-nodes"]["failure_fingerprint"] == workflows.normalize_failure(
        cases["compressed_nodes"]["status"]["message"]
    )[1]
    assert snapshot_by_key["remote", "uid-event"]["trigger_kind"] == "event"
    assert snapshot_by_key["remote", "uid-event"]["trigger_name"] == "pull-request-trigger"
    assert snapshot_by_key["remote", "uid-completed"]["phase"] == "Succeeded"
    assert snapshot_by_key["remote", "uid-completed"]["duration_seconds"] == 62
    assert snapshot_by_key["remote", "uid-completed"]["resources_duration_cpu"] == 31
    assert snapshot_by_key["remote", "uid-completed"]["resources_duration_memory"] == 605

    runs_by_key = {(row["cluster"], row["uid"]): row for row in runs}
    assert set(runs_by_key) == {
        ("local", "uid-reaped"),
        ("local", "uid-namespaced-template"),
        ("local", "uid-compressed-nodes"),
        ("remote", "uid-event"),
        ("remote", "uid-completed"),
    }
    assert runs_by_key["remote", "uid-completed"]["first_seen_at"] == "2026-09-22T18:00:00Z"
    assert runs_by_key["remote", "uid-completed"]["last_seen_at"] == generated_at
    assert runs_by_key["remote", "uid-completed"]["phase"] == "Succeeded"
    assert runs_by_key["local", "uid-reaped"]["first_seen_at"] == "2026-09-22T20:00:00Z"
    assert runs_by_key["local", "uid-reaped"]["last_seen_at"] == "2026-09-22T21:00:00Z"

    # The same observation feeds both publications. This checks the fallback
    # failure message, its classification/fingerprint, and nulls for a clean
    # run at the cycle boundary rather than only in the normalizer unit tests.
    for key in snapshot_by_key:
        assert (
            snapshot_by_key[key]["failure_fingerprint"]
            == runs_by_key[key]["failure_fingerprint"]
        )
        assert (
            snapshot_by_key[key]["failure_class"]
            == runs_by_key[key]["failure_class"]
        )
    assert snapshot_by_key["local", "uid-compressed-nodes"]["failure_class"] == "timeout"
    assert (
        snapshot_by_key["local", "uid-compressed-nodes"]["failure_fingerprint"]
        == workflows.normalize_failure(cases["compressed_nodes"]["status"]["message"])[1]
    )
    assert snapshot_by_key["remote", "uid-completed"]["failure_fingerprint"] is None
    assert snapshot_by_key["remote", "uid-completed"]["failure_class"] is None


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


def test_successful_zero_row_cycle_publishes_pairable_empty_snapshot(monkeypatch):
    """A confirmed empty listing is a generation, not an outage signal."""
    generated_at = "2026-09-23T19:00:00Z"
    cfg = _config([Cluster(name="ci"), Cluster(name="staging")])
    _list(monkeypatch, {"ci": ([], True), "staging": ([], False)})
    monkeypatch.setattr(main, "_now", lambda: generated_at)

    s3 = _MemoryS3(
        {
            "argo/data/workflows.parquet": parquet_io.table_to_parquet_bytes(
                [_workflow("old", "old")], parquet_io.WORKFLOWS_SCHEMA
            )
        }
    )

    assert main._run_cycle(cfg, s3) is True
    assert s3.puts == 3

    workflows_bytes = s3.objects["argo/data/workflows.parquet"]
    runs_bytes = s3.objects["argo/data/runs.parquet"]
    meta = json.loads(s3.objects["argo/data/meta.json"])
    meta_schema.validate(meta)

    snapshot = parquet_io.parquet_bytes_to_table(
        workflows_bytes, parquet_io.WORKFLOWS_SCHEMA
    )
    assert snapshot.schema == parquet_io.WORKFLOWS_SCHEMA
    assert snapshot.num_rows == 0
    assert meta["generated_at"] == generated_at
    assert meta["workflows"] == 0
    assert {
        stat["name"]: (stat["ok"], stat["workflows"])
        for stat in meta["clusters"]
    } == {"ci": (True, 0), "staging": (False, 0)}
    assert _paired(meta, workflows_bytes, runs_bytes)
    assert parquet_io.read_generation_id(workflows_bytes) == meta["generation_id"]


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


def test_partial_pagination_failure_isolated_from_successful_cluster(monkeypatch):
    """A later-page failure drops the partial answer but not prior history."""
    generated_at = "2026-09-27T12:00:00Z"
    cfg = replace(
        _config(
            [
                Cluster(name="healthy", base_url="http://healthy.example"),
                Cluster(name="flaky", base_url="http://flaky.example"),
            ]
        ),
        page_size=1,
    )
    monkeypatch.setattr(main, "_now", lambda: generated_at)
    monkeypatch.setattr(ledger, "_cutoff", lambda _retention_days: "2026-09-20T00:00:00Z")

    pages = {
        "http://healthy.example": {
            None: {"items": [_workflow("healthy-current", "healthy-current")]}
        },
        "http://flaky.example": {
            None: {
                "items": [_workflow("flaky-partial", "flaky-partial")],
                "metadata": {"continue": "flaky-next"},
            },
            "flaky-next": SimpleNamespace(status_code=503),
        },
    }
    calls = []

    def fake_get(url, params=None, **kwargs):
        params = dict(params or {})
        calls.append((url, params, kwargs))
        cluster_url = url.split("/apis/", 1)[0]
        page = pages[cluster_url][params.get("continue")]
        if isinstance(page, SimpleNamespace):
            return page
        return SimpleNamespace(status_code=200, json=lambda: page)

    monkeypatch.setattr(k8s_api.requests, "get", fake_get)

    prior_runs = [
        {
            "uid": "flaky-old-running",
            "cluster": "flaky",
            "phase": "Running",
            "first_seen_at": "2026-09-25T10:00:00Z",
            "last_seen_at": "2026-09-26T11:00:00Z",
        },
        {
            "uid": "flaky-old-completed",
            "cluster": "flaky",
            "phase": "Succeeded",
            "first_seen_at": "2026-09-24T10:00:00Z",
            "last_seen_at": "2026-09-25T11:00:00Z",
        },
        {
            "uid": "healthy-old",
            "cluster": "healthy",
            "phase": "Succeeded",
            "first_seen_at": "2026-09-23T10:00:00Z",
            "last_seen_at": "2026-09-24T11:00:00Z",
        },
    ]
    s3 = _MemoryS3(
        {
            "argo/data/runs.parquet": parquet_io.table_to_parquet_bytes(
                prior_runs, parquet_io.RUNS_SCHEMA
            )
        }
    )

    assert main._run_cycle(cfg, s3) is True
    assert calls == [
        (
            "http://healthy.example/apis/argoproj.io/v1alpha1/workflows",
            {"limit": 1},
            {"timeout": 10},
        ),
        (
            "http://flaky.example/apis/argoproj.io/v1alpha1/workflows",
            {"limit": 1},
            {"timeout": 10},
        ),
        (
            "http://flaky.example/apis/argoproj.io/v1alpha1/workflows",
            {"limit": 1, "continue": "flaky-next"},
            {"timeout": 10},
        ),
    ]

    workflows_bytes = s3.objects["argo/data/workflows.parquet"]
    runs_bytes = s3.objects["argo/data/runs.parquet"]
    meta = json.loads(s3.objects["argo/data/meta.json"])
    snapshot = parquet_io.parquet_bytes_to_table(
        workflows_bytes, parquet_io.WORKFLOWS_SCHEMA
    ).to_pylist()
    runs = parquet_io.parquet_bytes_to_table(runs_bytes, parquet_io.RUNS_SCHEMA).to_pylist()

    assert {(row["cluster"], row["uid"]) for row in snapshot} == {
        ("healthy", "healthy-current")
    }
    assert {row["uid"] for row in snapshot}.isdisjoint(
        {"flaky-partial", "flaky-old-running", "flaky-old-completed"}
    )
    runs_by_key = {(row["cluster"], row["uid"]): row for row in runs}
    assert runs_by_key["flaky", "flaky-old-running"]["last_seen_at"] == "2026-09-26T11:00:00Z"
    assert runs_by_key["flaky", "flaky-old-completed"]["last_seen_at"] == "2026-09-25T11:00:00Z"
    assert ("flaky", "flaky-partial") not in runs_by_key
    assert runs_by_key["healthy", "healthy-current"]["last_seen_at"] == generated_at

    assert {
        stat["name"]: (stat["ok"], stat["workflows"])
        for stat in meta["clusters"]
    } == {"healthy": (True, 1), "flaky": (False, 0)}
    assert meta["workflows"] == 1
    assert meta["runs"] == 4
    assert _paired(meta, workflows_bytes, runs_bytes)


def test_unreachable_cluster_ledger_rows_follow_last_seen_retention(monkeypatch):
    """A failed cluster is absent from the snapshot, but its ledger ages normally."""
    generated_at = "2026-09-27T12:00:00Z"
    cutoff = "2026-09-20T00:00:00Z"
    monkeypatch.setattr(main, "_now", lambda: generated_at)
    monkeypatch.setattr(ledger, "_cutoff", lambda _retention_days: cutoff)
    _list(
        monkeypatch,
        {
            "healthy": ([_workflow("healthy-current", "healthy-current")], True),
            "unreachable": ([], False),
        },
    )

    prior_runs = [
        {
            "uid": "healthy-current",
            "cluster": "healthy",
            "name": "healthy-old-name",
            "phase": "Running",
            "first_seen_at": "2026-09-01T10:00:00Z",
            "last_seen_at": "2026-09-19T10:00:00Z",
        },
        {
            "uid": "unreachable-retained",
            "cluster": "unreachable",
            "name": "unreachable-retained",
            "phase": "Failed",
            "message": "prior failure",
            "first_seen_at": "2026-09-10T10:00:00Z",
            "last_seen_at": cutoff,
        },
        {
            "uid": "unreachable-expired",
            "cluster": "unreachable",
            "name": "unreachable-expired",
            "phase": "Succeeded",
            "first_seen_at": "2026-09-09T10:00:00Z",
            "last_seen_at": "2026-09-19T23:59:59Z",
        },
    ]
    s3 = _MemoryS3(
        {
            "argo/data/runs.parquet": parquet_io.table_to_parquet_bytes(
                prior_runs, parquet_io.RUNS_SCHEMA
            )
        }
    )

    cfg = _config([Cluster(name="healthy"), Cluster(name="unreachable")])
    assert main._run_cycle(cfg, s3) is True

    workflows_bytes = s3.objects["argo/data/workflows.parquet"]
    runs_bytes = s3.objects["argo/data/runs.parquet"]
    meta = json.loads(s3.objects["argo/data/meta.json"])
    snapshot = parquet_io.parquet_bytes_to_table(
        workflows_bytes, parquet_io.WORKFLOWS_SCHEMA
    ).to_pylist()
    runs = parquet_io.parquet_bytes_to_table(runs_bytes, parquet_io.RUNS_SCHEMA).to_pylist()

    assert {(row["cluster"], row["uid"]) for row in snapshot} == {
        ("healthy", "healthy-current")
    }
    runs_by_key = {(row["cluster"], row["uid"]): row for row in runs}
    assert runs_by_key["healthy", "healthy-current"]["first_seen_at"] == "2026-09-01T10:00:00Z"
    assert runs_by_key["healthy", "healthy-current"]["last_seen_at"] == generated_at
    assert runs_by_key["healthy", "healthy-current"]["name"] == "healthy-current"
    assert {
        key: runs_by_key["unreachable", "unreachable-retained"][key]
        for key in ("phase", "message", "first_seen_at", "last_seen_at")
    } == {
        "phase": "Failed",
        "message": "prior failure",
        "first_seen_at": "2026-09-10T10:00:00Z",
        "last_seen_at": cutoff,
    }
    assert ("unreachable", "unreachable-expired") not in runs_by_key
    assert ("unreachable", "unreachable-retained") not in {
        (row["cluster"], row["uid"]) for row in snapshot
    }

    assert {
        stat["name"]: (stat["ok"], stat["workflows"])
        for stat in meta["clusters"]
    } == {"healthy": (True, 1), "unreachable": (False, 0)}
    assert meta["workflows"] == 1
    assert meta["runs"] == 2
    assert _paired(meta, workflows_bytes, runs_bytes)


_GENERATED_AT = "2026-09-23T19:00:00Z"
_OLD_GEN = f"{_GENERATED_AT}-000000aaaaaa"
_NEW_GENERATED_AT = "2026-09-23T19:01:00Z"


def _one_cluster_state(monkeypatch, s3):
    """A cycle listing one cluster with one workflow, publishing over `s3`."""
    _list(monkeypatch, {"ci": ([_workflow("wf-new", "wf-new")], True)})
    monkeypatch.setattr(main, "_now", lambda: _NEW_GENERATED_AT)
    return main._run_cycle(_config([Cluster(name="ci")]), s3)


def test_cycle_upgrades_a_legacy_runs_fixture_and_publishes_current_schema(monkeypatch):
    """The first cycle after a schema release upgrades the stored ledger."""
    legacy_schema = pa.schema(
        [
            field
            for field in parquet_io.RUNS_SCHEMA
            if field.name not in ("failure_fingerprint", "failure_class")
        ]
    )
    fixture = json.loads(
        (Path(__file__).with_name("fixtures") / "runs_pre_taxonomy.json").read_text(
            encoding="utf-8"
        )
    )
    generated_at = "2026-09-27T12:00:00Z"
    monkeypatch.setattr(main, "_now", lambda: generated_at)
    monkeypatch.setattr(ledger, "_cutoff", lambda _retention_days: "2026-09-01T00:00:00Z")
    _list(
        monkeypatch,
        {
            "ci": (
                [
                    _workflow(
                        "wf-reobserved",
                        "legacy-build-abcde",
                        phase="Failed",
                        message="error: test command failed",
                    )
                ],
                True,
            )
        },
    )
    s3 = _MemoryS3(
        {
            "argo/data/runs.parquet": parquet_io.table_to_parquet_bytes(
                fixture["rows"], legacy_schema
            )
        }
    )

    assert main._run_cycle(_config([Cluster(name="ci")]), s3) is True

    stored = parquet_io.parquet_bytes_to_table(
        s3.objects["argo/data/runs.parquet"], parquet_io.RUNS_SCHEMA
    )
    assert stored.schema == parquet_io.RUNS_SCHEMA
    rows = {row["uid"]: row for row in stored.to_pylist()}
    assert set(rows) == {"wf-reobserved", "wf-preserved"}
    assert rows["wf-reobserved"]["first_seen_at"] == "2026-09-25T10:00:05Z"
    assert rows["wf-reobserved"]["last_seen_at"] == generated_at
    assert rows["wf-reobserved"]["failure_fingerprint"] is not None
    assert rows["wf-reobserved"]["failure_class"] == "unknown"
    assert rows["wf-preserved"]["phase"] == "Succeeded"
    assert rows["wf-preserved"]["last_seen_at"] == "2026-09-24T11:01:05Z"
    assert rows["wf-preserved"]["failure_fingerprint"] is None
    assert rows["wf-preserved"]["failure_class"] is None


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


def test_first_cycle_without_runs_parquet_publishes_a_pairable_generation(monkeypatch):
    """A missing ledger is the normal empty state on the first cycle."""
    s3 = _RecordingS3()

    assert _one_cluster_state(monkeypatch, s3) is True

    assert s3.uploaded_keys == [
        "argo/data/workflows.parquet",
        "argo/data/runs.parquet",
        "argo/data/meta.json",
    ]
    runs = parquet_io.parquet_bytes_to_table(
        s3.objects["argo/data/runs.parquet"], parquet_io.RUNS_SCHEMA
    )
    assert [row["uid"] for row in runs.to_pylist()] == ["wf-new"]

    meta = json.loads(s3.objects["argo/data/meta.json"])
    assert meta["runs"] == 1
    assert _paired(
        meta,
        s3.objects["argo/data/workflows.parquet"],
        s3.objects["argo/data/runs.parquet"],
    )


def test_failed_collection_on_first_run_writes_nothing(monkeypatch):
    """A missing ledger is only normal after at least one cluster succeeds."""
    s3 = _RecordingS3()
    _list(monkeypatch, {"ci": ([], False)})

    assert main._run_cycle(_config([Cluster(name="ci")]), s3) is False

    assert s3.objects == {}
    assert s3.puts == 0
    assert s3.uploaded_keys == []


def test_malformed_cluster_does_not_block_publication_of_healthy_cluster(monkeypatch):
    cfg = _config([Cluster(name="broken"), Cluster(name="healthy")])
    _list(
        monkeypatch,
        {
            "broken": ([_workflow("wf-broken", "broken"), None], True),
            "healthy": ([_workflow("wf-healthy", "healthy")], True),
        },
    )
    s3 = _RecordingS3()

    assert main._run_cycle(cfg, s3) is True

    meta = json.loads(s3.objects["argo/data/meta.json"])
    assert meta["clusters"] == [
        {"name": "broken", "ok": False, "workflows": 0},
        {"name": "healthy", "ok": True, "workflows": 1},
    ]
    snapshot = parquet_io.parquet_bytes_to_table(
        s3.objects["argo/data/workflows.parquet"], parquet_io.WORKFLOWS_SCHEMA
    ).to_pylist()
    assert [row["uid"] for row in snapshot] == ["wf-healthy"]
    assert s3.uploaded_keys == [
        "argo/data/workflows.parquet",
        "argo/data/runs.parquet",
        "argo/data/meta.json",
    ]


def test_all_malformed_clusters_preserve_the_previous_publication(monkeypatch):
    cfg = _config([Cluster(name="broken-a"), Cluster(name="broken-b")])
    _list(
        monkeypatch,
        {
            "broken-a": ([_workflow("wf-a", "a"), None], True),
            "broken-b": ([{"metadata": {}, "spec": {}, "status": {}}], True),
        },
    )
    s3 = _MemoryS3(_prior_generation())
    before = dict(s3.objects)

    assert main._run_cycle(cfg, s3) is False

    assert s3.objects == before
    assert s3.puts == 0


def test_failed_listing_preserves_the_previous_committed_generation(monkeypatch):
    s3 = _MemoryS3(_prior_generation())
    before = dict(s3.objects)
    _list(monkeypatch, {"ci": ([], False)})

    assert main._run_cycle(_config([Cluster(name="ci")]), s3) is False

    assert s3.objects == before
    assert s3.puts == 0


def test_failed_serialization_preserves_the_previous_committed_generation(monkeypatch):
    s3 = _MemoryS3(_prior_generation())
    before = dict(s3.objects)
    _list(monkeypatch, {"ci": ([_workflow("wf-new", "wf-new")], True)})

    def fail_serialization(*_args, **_kwargs):
        raise RuntimeError("injected serialization failure")

    monkeypatch.setattr(main.parquet_io, "table_to_parquet_bytes", fail_serialization)

    with pytest.raises(RuntimeError, match="injected serialization failure") as raised:
        _one_cluster_state(monkeypatch, s3)

    assert raised.value.cycle_failure_phase == "compute"
    assert s3.objects == before
    assert s3.puts == 0


@pytest.mark.parametrize(
    ("failure", "expected_phase"),
    [
        pytest.param("collection", "read", id="cluster-collection"),
        pytest.param("ledger", "compute", id="ledger-folding"),
        pytest.param("parquet", "compute", id="parquet-serialization"),
        pytest.param("meta", "compute", id="meta-serialization"),
    ],
)
def test_pre_publish_cycle_failures_preserve_and_then_recover(
    monkeypatch, failure, expected_phase
):
    """Read/compute failures leave the prior generation available to retry."""
    cfg = _config([Cluster(name="ci")])
    s3 = _MemoryS3(_prior_generation())
    before = dict(s3.objects)
    _list(monkeypatch, {"ci": ([_workflow("wf-new", "wf-new")], True)})
    monkeypatch.setattr(ledger, "_cutoff", lambda _retention_days: _GENERATED_AT)
    enabled = True

    if failure == "collection":
        real_fetch = workflows.fetch_workflows

        def fail_collection(*args, **kwargs):
            if enabled:
                raise RuntimeError("injected cluster collection failure")
            return real_fetch(*args, **kwargs)

        monkeypatch.setattr(workflows, "fetch_workflows", fail_collection)
        message = "injected cluster collection failure"
    elif failure == "ledger":
        real_merge = ledger.merge

        def fail_ledger(*args, **kwargs):
            if enabled:
                raise RuntimeError("injected ledger folding failure")
            return real_merge(*args, **kwargs)

        monkeypatch.setattr(ledger, "merge", fail_ledger)
        message = "injected ledger folding failure"
    elif failure == "parquet":
        real_serialize = parquet_io.table_to_parquet_bytes

        def fail_parquet(*args, **kwargs):
            if enabled:
                raise RuntimeError("injected Parquet serialization failure")
            return real_serialize(*args, **kwargs)

        monkeypatch.setattr(parquet_io, "table_to_parquet_bytes", fail_parquet)
        message = "injected Parquet serialization failure"
    else:
        real_json_dumps = main.json.dumps

        def fail_meta(value, *args, **kwargs):
            if enabled:
                raise RuntimeError("injected meta serialization failure")
            return real_json_dumps(value, *args, **kwargs)

        monkeypatch.setattr(main.json, "dumps", fail_meta)
        message = "injected meta serialization failure"

    with pytest.raises(RuntimeError, match=message) as raised:
        main._run_cycle(cfg, s3)

    assert raised.value.cycle_failure_phase == expected_phase
    assert s3.puts == 0
    for key in main._PUBLICATION_OBJECTS:
        assert s3.objects[f"argo/data/{key}"] == before[f"argo/data/{key}"]

    enabled = False
    monkeypatch.setattr(main, "_now", lambda: _NEW_GENERATED_AT)
    assert main._run_cycle(cfg, s3) is True

    meta = json.loads(s3.objects["argo/data/meta.json"])
    meta_schema.validate(meta)
    assert meta["generated_at"] == _NEW_GENERATED_AT
    assert meta["generation_id"] != json.loads(before["argo/data/meta.json"])["generation_id"]
    assert _paired(
        meta,
        s3.objects["argo/data/workflows.parquet"],
        s3.objects["argo/data/runs.parquet"],
    )
    assert s3.puts == 3
    assert {
        s3.objects[f"argo/data/{key}"] != before[f"argo/data/{key}"]
        for key in main._PUBLICATION_OBJECTS
    } == {True}


@pytest.mark.parametrize(
    ("rows", "expected_workflows", "expected_runs"),
    [
        pytest.param(
            [_workflow("wf-new", "wf-new")],
            1,
            1,
            id="identical-non-empty-rows",
        ),
        pytest.param([], 0, 0, id="identical-zero-row-generations"),
    ],
)
def test_two_cycles_in_the_same_second_get_distinct_generation_ids(
    monkeypatch, rows, expected_workflows, expected_runs
):
    """generated_at has second resolution and the poll interval is operator
    configured; the random suffix is what keeps two cycles that do land in
    the same second from being mistaken for one generation. This remains true
    when the two snapshots contain the same rows, including empty snapshots.
    """
    monkeypatch.setattr(main, "_now", lambda: _GENERATED_AT)
    s3 = _MemoryS3()
    cfg = _config([Cluster(name="ci")])
    _list(monkeypatch, {"ci": (rows, True)})

    assert main._run_cycle(cfg, s3) is True
    first_meta = json.loads(s3.objects["argo/data/meta.json"])
    first_workflows = s3.objects["argo/data/workflows.parquet"]
    first_runs = s3.objects["argo/data/runs.parquet"]

    assert main._run_cycle(cfg, s3) is True
    second_meta = json.loads(s3.objects["argo/data/meta.json"])
    second_workflows = s3.objects["argo/data/workflows.parquet"]
    second_runs = s3.objects["argo/data/runs.parquet"]

    assert first_meta["generated_at"] == second_meta["generated_at"] == _GENERATED_AT
    assert first_meta["generation_id"] != second_meta["generation_id"]
    assert first_meta["workflows"] == second_meta["workflows"] == expected_workflows
    assert first_meta["runs"] == second_meta["runs"] == expected_runs
    assert first_meta["clusters"] == second_meta["clusters"] == [
        {"name": "ci", "ok": True, "workflows": expected_workflows}
    ]
    meta_schema.validate(first_meta)
    meta_schema.validate(second_meta)

    assert _paired(first_meta, first_workflows, first_runs)
    assert _paired(second_meta, second_workflows, second_runs)
    assert parquet_io.read_generation_id(first_workflows) == first_meta["generation_id"]
    assert parquet_io.read_generation_id(first_runs) == first_meta["generation_id"]
    assert parquet_io.read_generation_id(second_workflows) == second_meta["generation_id"]
    assert parquet_io.read_generation_id(second_runs) == second_meta["generation_id"]

    first_workflows_table = parquet_io.parquet_bytes_to_table(
        first_workflows, parquet_io.WORKFLOWS_SCHEMA
    )
    second_workflows_table = parquet_io.parquet_bytes_to_table(
        second_workflows, parquet_io.WORKFLOWS_SCHEMA
    )
    first_runs_table = parquet_io.parquet_bytes_to_table(first_runs, parquet_io.RUNS_SCHEMA)
    second_runs_table = parquet_io.parquet_bytes_to_table(second_runs, parquet_io.RUNS_SCHEMA)
    assert first_workflows_table.schema == second_workflows_table.schema == parquet_io.WORKFLOWS_SCHEMA
    assert first_runs_table.schema == second_runs_table.schema == parquet_io.RUNS_SCHEMA
    assert first_workflows_table.num_rows == second_workflows_table.num_rows == expected_workflows
    assert first_runs_table.num_rows == second_runs_table.num_rows == expected_runs
    assert first_workflows_table.to_pylist() == second_workflows_table.to_pylist()
    assert first_runs_table.to_pylist() == second_runs_table.to_pylist()



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


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(
            ClientError(
                {"Error": {"Code": "404", "Message": "injected"}}, "GetObject"
            ),
            id="generic-404",
        ),
        pytest.param(
            ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "injected"}},
                "GetObject",
            ),
            id="authentication-failure",
        ),
        pytest.param(
            ReadTimeoutError(endpoint_url="http://s3.example", error="injected"),
            id="timeout",
        ),
        pytest.param(
            ClientError(
                {"Error": {"Code": "InternalError", "Message": "injected"}},
                "GetObject",
            ),
            id="server-error",
        ),
    ],
)
def test_failed_ledger_read_writes_nothing_at_all(monkeypatch, error):
    s3 = _FailGet(_prior_generation(), error=error)
    before = dict(s3.objects)

    with pytest.raises(type(error)) as raised:
        _one_cluster_state(monkeypatch, s3)

    assert raised.value is error
    assert s3.objects == before
    assert s3.puts == 0


def test_malformed_ledger_object_writes_nothing_at_all(monkeypatch):
    objects = _prior_generation()
    objects["argo/data/runs.parquet"] = b"not parquet"
    s3 = _MemoryS3(objects)
    before = dict(s3.objects)

    with pytest.raises(pa.ArrowInvalid):
        _one_cluster_state(monkeypatch, s3)

    assert s3.objects == before
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


@pytest.mark.parametrize("fail_on_put", [1, 2, 3], ids=["workflows", "runs", "meta"])
def test_retry_after_each_partial_publication_preserves_commit_marker_and_ledger(
    monkeypatch, fail_on_put
):
    """A failed publication is invisible to a pairing-aware consumer until retry commits it."""
    s3 = _FailNthPut(_prior_generation(), fail_on_put=fail_on_put)
    old_meta_bytes = s3.objects["argo/data/meta.json"]
    old_generation = json.loads(old_meta_bytes)["generation_id"]

    with pytest.raises(ClientError) as raised:
        _one_cluster_state(monkeypatch, s3)

    assert raised.value.cycle_published_objects == list(
        main._PUBLICATION_OBJECTS[: fail_on_put - 1]
    )
    assert s3.objects["argo/data/meta.json"] == old_meta_bytes
    assert _consumer_generation(s3.objects, old_generation) == old_generation

    # Re-run the cycle against the same storage, including whatever the first
    # attempt managed to overwrite. The one-shot injector is exhausted, so
    # this is the successful retry.
    assert _one_cluster_state(monkeypatch, s3) is True

    final_meta = json.loads(s3.objects["argo/data/meta.json"])
    final_generation = final_meta["generation_id"]
    assert final_generation != old_generation
    assert _paired(
        final_meta,
        s3.objects["argo/data/workflows.parquet"],
        s3.objects["argo/data/runs.parquet"],
    )
    assert _consumer_generation(s3.objects, old_generation) == final_generation

    # Every successful PUT before the final meta.json PUT leaves the old
    # marker in place. A consumer therefore retains the old complete
    # generation instead of exposing either torn combination.
    assert len(s3.states_after_successful_put) >= 3
    for state in s3.states_after_successful_put[:-1]:
        assert state["argo/data/meta.json"] == old_meta_bytes
        assert _consumer_generation(state, old_generation) == old_generation
    assert s3.states_after_successful_put[-1]["argo/data/meta.json"] != old_meta_bytes

    runs = parquet_io.parquet_bytes_to_table(
        s3.objects["argo/data/runs.parquet"], parquet_io.RUNS_SCHEMA
    ).to_pylist()
    keys = [(row.get("cluster") or "", row["uid"]) for row in runs]
    assert len(keys) == len(set(keys))
    assert set(keys) == {("", "wf-old"), ("ci", "wf-new")}


def test_poll_loop_logs_cycle_failure_and_continues_at_the_configured_interval(
    monkeypatch, caplog
):
    cfg = _config([Cluster(name="ci")])
    s3 = object()
    attempts = []
    waits = []

    class Stop:
        def __init__(self):
            self.stopped = False

        def is_set(self):
            return self.stopped

        def wait(self, interval):
            waits.append(interval)
            if len(attempts) >= 2:
                self.stopped = True
            return self.stopped

    class Health:
        def __init__(self):
            self.successes = 0

        def record_success(self):
            self.successes += 1

    stop = Stop()
    health = Health()

    def cycle(_cfg, _s3):
        attempts.append(len(attempts) + 1)
        if len(attempts) == 1:
            raise RuntimeError("injected cycle failure")
        return True

    monkeypatch.setattr(main, "_run_cycle", cycle)
    with caplog.at_level("ERROR", logger="src.main"):
        main._run_poll_loop(cfg, s3, stop, health)

    assert attempts == [1, 2]
    assert waits == [300, 300]
    assert health.successes == 1
    assert "cycle failed, will retry next interval" in caplog.text
    assert "injected cycle failure" in caplog.text


def test_poll_loop_keeps_cycles_non_overlapping_and_waits_after_completion(
    monkeypatch,
):
    cfg = replace(_config([Cluster(name="ci")]), poll_interval_seconds=5)
    events = []
    clock = [0]

    class Stop:
        def __init__(self):
            self.stopped = False

        def is_set(self):
            return self.stopped

        def wait(self, interval):
            events.append(("wait", clock[0], interval))
            clock[0] += interval
            if len([event for event in events if event[0] == "start"]) == 2:
                self.stopped = True
            return self.stopped

    class Health:
        def record_success(self):
            events.append(("success", clock[0]))

    def cycle(_cfg, _s3):
        # A cycle that takes longer than the configured delay. The second
        # start must still wait for this cycle to finish and for the delay.
        events.append(("start", clock[0]))
        clock[0] += 7
        events.append(("finish", clock[0]))
        return True

    monkeypatch.setattr(main, "_run_cycle", cycle)
    main._run_poll_loop(cfg, object(), Stop(), Health())

    assert events == [
        ("start", 0),
        ("finish", 7),
        ("success", 7),
        ("wait", 7, 5),
        ("start", 12),
        ("finish", 19),
        ("success", 19),
        ("wait", 19, 5),
    ]


def test_poll_loop_retries_failed_cycles_after_the_same_delay(monkeypatch):
    cfg = replace(_config([Cluster(name="ci")]), poll_interval_seconds=5)
    events = []
    clock = [0]

    class Stop:
        def __init__(self):
            self.stopped = False

        def is_set(self):
            return self.stopped

        def wait(self, interval):
            events.append(("wait", clock[0], interval))
            clock[0] += interval
            self.stopped = len([event for event in events if event[0] == "start"]) == 2
            return self.stopped

    class Health:
        def record_success(self):
            events.append(("success", clock[0]))

    def cycle(_cfg, _s3):
        events.append(("start", clock[0]))
        clock[0] += 2
        if len([event for event in events if event[0] == "start"]) == 1:
            raise RuntimeError("injected cycle failure")
        events.append(("finish", clock[0]))
        return True

    monkeypatch.setattr(main, "_run_cycle", cycle)
    main._run_poll_loop(cfg, object(), Stop(), Health())

    assert events == [
        ("start", 0),
        ("wait", 2, 5),
        ("start", 7),
        ("finish", 9),
        ("success", 9),
        ("wait", 9, 5),
    ]


def test_poll_loop_observes_terminal_workflow_before_ttl_deletion(monkeypatch):
    """The TTL guarantee uses the effective start-to-start cadence."""
    cfg = replace(_config([Cluster(name="ci")]), poll_interval_seconds=3)
    clock = [0]
    observations = []
    terminal_at = 1
    ttl_seconds = 7
    cycle_duration = 4

    class Stop:
        def __init__(self):
            self.stopped = False

        def is_set(self):
            return self.stopped

        def wait(self, interval):
            clock[0] += interval
            self.stopped = len(observations) == 2
            return self.stopped

    class Health:
        def record_success(self):
            pass

    def cycle(_cfg, _s3):
        if clock[0] < terminal_at:
            phase = "Running"
        elif clock[0] < terminal_at + ttl_seconds:
            phase = "Succeeded"
        else:
            phase = "Deleted"
        observations.append((clock[0], phase))
        clock[0] += cycle_duration
        return True

    monkeypatch.setattr(main, "_run_cycle", cycle)
    main._run_poll_loop(cfg, object(), Stop(), Health())

    # The first cycle starts at t=0 and takes 4s; the 3s post-cycle delay
    # starts the next one at t=7. The terminal object survives until t=8.
    assert observations == [(0, "Running"), (7, "Succeeded")]


@pytest.mark.parametrize(
    ("log_level", "cycle_failure_is_logged"),
    [("ERROR", True), ("CRITICAL", False)],
)
def test_configured_log_level_controls_cycle_failure_logging(
    monkeypatch, caplog, log_level, cycle_failure_is_logged
):
    cfg = replace(_config([Cluster(name="ci")]), log_level=log_level)
    monkeypatch.setattr(main.config, "load", lambda: cfg)
    monkeypatch.setattr(main.s3io, "client", lambda _endpoint: object())
    monkeypatch.setattr(main, "_serve_health", lambda _port, _state: None)
    monkeypatch.setattr(main, "_stop_health", lambda _server: None)
    real_poll_loop = main._run_poll_loop

    def run_one_cycle_then_stop(_cfg, _s3, _stop, health):
        class Stop:
            def __init__(self):
                self.stopped = False

            def is_set(self):
                return self.stopped

            def wait(self, _interval):
                self.stopped = True
                return True

        def failed_cycle(_cfg, _s3):
            raise RuntimeError("injected cycle failure")

        monkeypatch.setattr(main, "_run_cycle", failed_cycle)
        real_poll_loop(cfg, object(), Stop(), health)

    monkeypatch.setattr(main, "_run_poll_loop", run_one_cycle_then_stop)
    with caplog.at_level(logging.NOTSET):
        main.main()

    failure_records = [
        record
        for record in caplog.records
        if record.name == "src.main"
        and "cycle failed, will retry next interval" in record.getMessage()
    ]
    assert bool(failure_records) is cycle_failure_is_logged
    if cycle_failure_is_logged:
        assert str(failure_records[0].exc_info[1]) == "injected cycle failure"


def test_failed_read_cycle_logs_clusters_phase_and_skipped_publication(monkeypatch, caplog):
    cfg = _config([Cluster(name="ci"), Cluster(name="staging")])
    _list(monkeypatch, {"ci": ([], False), "staging": ([], False)})

    with caplog.at_level("ERROR", logger="src.main"):
        assert main._run_cycle(cfg, _MemoryS3()) is False

    assert "failure_phase=read" in caplog.text
    assert "affected_clusters=ci,staging" in caplog.text
    assert "failed_clusters=ci,staging" in caplog.text
    assert "publication=skipped" in caplog.text
    assert "published_objects=none" in caplog.text
    assert "skipped_objects=workflows.parquet,runs.parquet,meta.json" in caplog.text


def test_failed_publish_logs_phase_and_partial_publication(monkeypatch, caplog):
    s3 = _FailNthPut(_MemoryS3().objects, fail_on_put=2)
    _list(monkeypatch, {"ci": ([_workflow("wf", "wf")], True)})

    with caplog.at_level("ERROR", logger="src.main"), pytest.raises(ClientError):
        main._run_cycle(_config([Cluster(name="ci")]), s3)

    assert "failure_phase=publish" in caplog.text
    assert "affected_clusters=ci" in caplog.text
    assert "publication=partial" in caplog.text
    assert "published_objects=workflows.parquet" in caplog.text
    assert "skipped_objects=runs.parquet,meta.json" in caplog.text


def test_poll_loop_exits_after_shutdown_during_sleep(monkeypatch):
    cfg = _config([Cluster(name="ci")])
    attempts = []
    waits = []

    class Stop:
        def __init__(self):
            self.stopped = False

        def set(self):
            self.stopped = True

        def is_set(self):
            return self.stopped

        def wait(self, interval):
            waits.append(interval)
            self.set()
            return True

    class Health:
        def __init__(self):
            self.successes = 0

        def record_success(self):
            self.successes += 1

    def cycle(_cfg, _s3):
        attempts.append(len(attempts) + 1)
        return True

    stop = Stop()
    health = Health()
    monkeypatch.setattr(main, "_run_cycle", cycle)
    main._run_poll_loop(cfg, object(), stop, health)

    assert attempts == [1]
    assert waits == [300]
    assert stop.is_set()
    assert health.successes == 1


def test_shutdown_during_active_cycle_drains_one_paired_generation(monkeypatch):
    cfg = _config([Cluster(name="ci")])
    cycle_calls = []
    waits = []

    class Stop:
        def __init__(self):
            self.stopped = False

        def set(self):
            self.stopped = True

        def is_set(self):
            return self.stopped

        def wait(self, interval):
            waits.append(interval)
            return self.is_set()

    class Health:
        def __init__(self):
            self.successes = 0

        def record_success(self):
            self.successes += 1

    stop = Stop()
    health = Health()

    class ShutdownOnFirstPut(_MemoryS3):
        def put_object(self, Bucket, Key, Body, ContentType):
            super().put_object(Bucket, Key, Body, ContentType)
            if Key == "argo/data/workflows.parquet":
                stop.set()

    _list(monkeypatch, {"ci": ([_workflow("wf-new", "wf-new")], True)})
    monkeypatch.setattr(main, "_now", lambda: _NEW_GENERATED_AT)
    real_cycle = main._run_cycle

    def counted_cycle(cycle_cfg, cycle_s3):
        cycle_calls.append(len(cycle_calls) + 1)
        return real_cycle(cycle_cfg, cycle_s3)

    monkeypatch.setattr(main, "_run_cycle", counted_cycle)
    s3 = ShutdownOnFirstPut(_prior_generation())
    main._run_poll_loop(cfg, s3, stop, health)

    meta = json.loads(s3.objects["argo/data/meta.json"])
    assert cycle_calls == [1]
    assert stop.is_set()
    assert waits == [300]
    assert health.successes == 1
    assert s3.puts == 3
    assert meta["generation_id"].startswith(_NEW_GENERATED_AT)
    assert meta["generation_id"] != _OLD_GEN
    assert _paired(
        meta,
        s3.objects["argo/data/workflows.parquet"],
        s3.objects["argo/data/runs.parquet"],
    )


def test_incomplete_cycle_does_not_advance_the_health_heartbeat(monkeypatch):
    cfg = _config([Cluster(name="ci")])

    class Stop:
        def __init__(self):
            self.stopped = False

        def is_set(self):
            return self.stopped

        def wait(self, _interval):
            self.stopped = True
            return True

    class Health:
        def __init__(self):
            self.successes = 0

        def record_success(self):
            self.successes += 1

    health = Health()
    monkeypatch.setattr(main, "_run_cycle", lambda _cfg, _s3: False)
    main._run_poll_loop(cfg, object(), Stop(), health)

    assert health.successes == 0


def _health_request(port, path="/health"):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        return response.status, dict(response.getheaders()), response.read()
    finally:
        connection.close()


def test_health_endpoint_reports_starting_success_and_stale_heartbeat():
    now = [100.0]
    state = main._HealthState(
        10,
        monotonic=lambda: now[0],
        wall_clock=lambda: 1_767_225_600.0,
    )
    server = main._serve_health(0, state)
    port = server.server_address[1]

    try:
        code, headers, body = _health_request(port)
        assert code == 503
        assert headers["Content-Type"] == "application/json"
        assert headers["Cache-Control"] == "no-store"
        assert json.loads(body) == {"status": "starting", "last_success_at": None}

        state.record_success()
        code, headers, body = _health_request(port)
        assert code == 200
        assert headers["Content-Type"] == "application/json"
        assert headers["Cache-Control"] == "no-store"
        assert json.loads(body) == {
            "status": "ok",
            "last_success_at": "2026-01-01T00:00:00Z",
            "age_seconds": 0.0,
        }

        now[0] = 119.999
        code, _, body = _health_request(port)
        assert code == 200
        assert json.loads(body) == {
            "status": "ok",
            "last_success_at": "2026-01-01T00:00:00Z",
            "age_seconds": 19.999,
        }

        now[0] = 120.0
        code, _, body = _health_request(port)
        assert code == 503
        assert json.loads(body) == {
            "status": "stale",
            "last_success_at": "2026-01-01T00:00:00Z",
            "age_seconds": 20.0,
        }

        state.record_success()
        code, _, body = _health_request(port)
        assert code == 200
        assert json.loads(body) == {
            "status": "ok",
            "last_success_at": "2026-01-01T00:00:00Z",
            "age_seconds": 0.0,
        }
    finally:
        main._stop_health(server)


def test_health_server_can_be_stopped_before_its_first_request():
    server = main._serve_health(0)
    main._stop_health(server)


def test_main_stops_health_server_and_restores_signal_handlers(monkeypatch):
    cfg = _config([Cluster(name="ci")])
    monkeypatch.setattr(main.config, "load", lambda: cfg)
    monkeypatch.setattr(main.s3io, "client", lambda _endpoint: object())

    old_handlers = {
        main.signal.SIGTERM: "old-term-handler",
        main.signal.SIGINT: "old-int-handler",
    }
    installed = {}
    signal_calls = []

    def fake_signal(signum, handler):
        signal_calls.append((signum, handler))
        if handler not in installed.values():
            installed[signum] = handler
        return old_handlers[signum]

    monkeypatch.setattr(main.signal, "signal", fake_signal)

    class Server:
        thread = None

        def __init__(self):
            self.shutdown_called = False
            self.close_called = False

        def shutdown(self):
            self.shutdown_called = True

        def server_close(self):
            self.close_called = True

    server = Server()
    served = []

    def fake_serve(port, state):
        served.append((port, state))
        return server

    def fake_loop(_cfg, _s3, stop, state):
        served.append(stop)
        installed[main.signal.SIGTERM](main.signal.SIGTERM, None)
        assert stop.is_set()

    monkeypatch.setattr(main, "_serve_health", fake_serve)
    monkeypatch.setattr(main, "_run_poll_loop", fake_loop)

    main.main()

    assert served[0][0] == cfg.health_port
    assert isinstance(served[0][1], main._HealthState)
    assert served[1].is_set()
    assert server.shutdown_called
    assert server.close_called
    assert signal_calls[-2:] == [
        (main.signal.SIGTERM, old_handlers[main.signal.SIGTERM]),
        (main.signal.SIGINT, old_handlers[main.signal.SIGINT]),
    ]
