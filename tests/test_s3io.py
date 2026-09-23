"""The S3 client's retry behavior is part of the publication contract: it is
the retry layer under docs/notes/atomic-publication.md, and it must not drift
back to whatever the installed botocore version happens to default to."""

from src import s3io
from src.config import S3Endpoint


def _endpoint():
    return S3Endpoint(
        endpoint_url="http://s3.example",
        access_key_id="access",
        secret_access_key="secret",
        bucket="bucket",
        addressing_style="path",
        region="us-east-1",
    )


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
