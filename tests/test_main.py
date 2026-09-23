import io
import json

from botocore.exceptions import ClientError

from src import main, parquet_io, workflows
from src.config import Cluster, Config, S3Endpoint


class _MemoryS3:
    def __init__(self, objects=None):
        self.objects = dict(objects or {})

    def get_object(self, Bucket, Key):
        try:
            data = self.objects[Key]
        except KeyError:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "GetObject"
            ) from None
        return {"Body": io.BytesIO(data)}

    def put_object(self, Bucket, Key, Body, ContentType):
        self.objects[Key] = Body


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
