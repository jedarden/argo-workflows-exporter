"""The S3 client's retry behavior is part of the publication contract: it is
the retry layer under docs/notes/atomic-publication.md, and it must not drift
back to whatever the installed botocore version happens to default to."""

import io

import pytest
from botocore.exceptions import ClientError

from src import main, s3io, workflows
from src.config import Cluster, Config, S3Endpoint


def _endpoint(addressing_style="path", region="us-east-1"):
    return S3Endpoint(
        endpoint_url="http://s3.example",
        access_key_id="test-access",
        secret_access_key="test-secret",
        bucket="bucket",
        addressing_style=addressing_style,
        region=region,
    )


@pytest.mark.parametrize("addressing_style", ["virtual", "path"])
def test_client_forwards_endpoint_region_and_addressing(monkeypatch, addressing_style):
    captured = {}
    client = object()

    def fake_client(service, **kwargs):
        captured["service"] = service
        captured.update(kwargs)
        return client

    monkeypatch.setattr(s3io.boto3, "client", fake_client)
    endpoint = _endpoint(addressing_style=addressing_style, region="eu-west-2")

    assert s3io.client(endpoint) is client
    assert captured["service"] == "s3"
    assert captured["endpoint_url"] == endpoint.endpoint_url
    assert captured["region_name"] == endpoint.region
    assert captured["config"].s3 == {"addressing_style": addressing_style}
    assert captured["aws_access_key_id"] == endpoint.access_key_id
    assert captured["aws_secret_access_key"] == endpoint.secret_access_key


def test_client_pins_standard_mode_retries(monkeypatch):
    captured = {}

    def fake_client(service, config=None, **kwargs):
        captured["service"] = service
        captured["config"] = config
        return object()

    monkeypatch.setattr(s3io.boto3, "client", fake_client)
    s3io.client(_endpoint())

    assert captured["service"] == "s3"
    assert captured["config"].retries == {"max_attempts": 10, "mode": "standard"}


def test_download_bytes_reads_the_requested_object():
    calls = []

    class S3:
        def get_object(self, **kwargs):
            calls.append(kwargs)
            return {"Body": io.BytesIO(b"stored")}

    assert s3io.download_bytes(S3(), "bucket", "argo/data/runs.parquet") == b"stored"
    assert calls == [{"Bucket": "bucket", "Key": "argo/data/runs.parquet"}]


@pytest.mark.parametrize("code", ["NoSuchKey", "404"])
def test_download_bytes_treats_a_missing_runs_object_as_a_first_run(code):
    class S3:
        def get_object(self, **kwargs):
            raise ClientError(
                {"Error": {"Code": code, "Message": "missing"}}, "GetObject"
            )

    assert s3io.download_bytes(S3(), "bucket", "argo/data/runs.parquet") is None


@pytest.mark.parametrize("code", ["AccessDenied", "InternalError"])
def test_download_bytes_propagates_non_missing_client_errors(code):
    error = ClientError(
        {"Error": {"Code": code, "Message": "injected"}}, "GetObject"
    )

    class S3:
        def get_object(self, **kwargs):
            raise error

    with pytest.raises(ClientError) as raised:
        s3io.download_bytes(S3(), "bucket", "argo/data/runs.parquet")

    assert raised.value is error


def test_download_bytes_propagates_non_client_errors():
    error = RuntimeError("injected")

    class S3:
        def get_object(self, **kwargs):
            raise error

    with pytest.raises(RuntimeError) as raised:
        s3io.download_bytes(S3(), "bucket", "argo/data/runs.parquet")

    assert raised.value is error


def test_upload_bytes_sends_payload_and_content_type():
    calls = []
    data = b"payload"

    class S3:
        def put_object(self, **kwargs):
            calls.append(kwargs)

    s3io.upload_bytes(S3(), "bucket", "argo/data/runs.parquet", data, "application/octet-stream")

    assert calls == [
        {
            "Bucket": "bucket",
            "Key": "argo/data/runs.parquet",
            "Body": data,
            "ContentType": "application/octet-stream",
        }
    ]


def test_upload_bytes_propagates_client_errors():
    error = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "injected"}}, "PutObject"
    )

    class S3:
        def put_object(self, **kwargs):
            raise error

    with pytest.raises(ClientError) as raised:
        s3io.upload_bytes(S3(), "bucket", "argo/data/runs.parquet", b"payload", "text/plain")

    assert raised.value is error


def _config(prefix):
    return Config(
        clusters=[Cluster(name="ci")],
        namespace="",
        dest=_endpoint(),
        dest_prefix=prefix,
        version="test",
        poll_interval_seconds=300,
        run_retention_days=7,
        http_timeout_seconds=10,
        page_size=500,
        health_port=8080,
        log_level="INFO",
    )


def test_cycle_uses_dest_prefix_for_first_run_reads_and_all_output_keys(monkeypatch):
    calls = []

    class S3:
        def get_object(self, **kwargs):
            calls.append(kwargs)
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "GetObject"
            )

        def put_object(self, **kwargs):
            calls.append(kwargs)

    monkeypatch.setattr(
        workflows,
        "fetch_workflows",
        lambda *args: ([], [{"name": "ci", "ok": True, "workflows": 0}]),
    )

    assert main._run_cycle(_config("tenant/argo"), S3()) is True
    assert [call["Key"] for call in calls] == [
        "tenant/argo/runs.parquet",
        "tenant/argo/workflows.parquet",
        "tenant/argo/runs.parquet",
        "tenant/argo/meta.json",
    ]
