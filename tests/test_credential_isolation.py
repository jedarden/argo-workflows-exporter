"""Integration coverage for credential-source isolation.

Three credential sources exist in this exporter and the README promises they
never mix ("Access required"): the local cluster's projected ServiceAccount
files, nothing at all for proxied clusters, and the ``DEST_S3_*``
environment variables for the destination bucket. These tests load a mixed
local/remote configuration through ``config.load()`` with decoy credentials
planted in the wider environment, then follow every credential to the
request that carries it — so a regression that let one source fall back to
another (or to an ambient ``AWS_*`` default) fails here instead of shipping
the wrong credential to a cluster or bucket.
"""

import json
from types import SimpleNamespace

from botocore.exceptions import ClientError

from src import config, k8s_api, main, s3io

_SA_TOKEN = "projected-service-account-token"
_SA_CA = "projected-ca-certificate"
_S3_ACCESS_KEY = "dest-s3-access-key"
_S3_SECRET_KEY = "dest-s3-secret-key"
_S3_ENDPOINT = "http://dest-s3.example"
_S3_BUCKET = "dest-s3-bucket"

# Credentials a fallback regression would reach for. botocore consults its
# ambient environment chain whenever explicit credentials, region, or
# endpoint are missing, so these are planted where ``config.load()`` and the
# clients it builds can see them: a decoy value reaching any request proves
# the wrong source won.
_AMBIENT_DECOYS = {
    "AWS_ACCESS_KEY_ID": "ambient-aws-access-key",
    "AWS_SECRET_ACCESS_KEY": "ambient-aws-secret-key",
    "AWS_SESSION_TOKEN": "ambient-aws-session-token",
    "AWS_DEFAULT_REGION": "ambient-region-1",
    "AWS_ENDPOINT_URL": "http://ambient-endpoint.example",
    "AWS_ENDPOINT_URL_S3": "http://ambient-s3-endpoint.example",
}

# Everything this test run must own exclusively: the exporter's own
# variables and the ambient AWS family, cleared so neither a host shell nor
# a real ``~/.aws`` fallback can leak a value into an exact-credential run.
_OWNED_ENV_KEYS = (
    "CLUSTERS_JSON",
    "DEST_S3_ACCESS_KEY_ID",
    "DEST_S3_ADDRESSING_STYLE",
    "DEST_S3_BUCKET",
    "DEST_S3_ENDPOINT",
    "DEST_S3_PREFIX",
    "DEST_S3_REGION",
    "DEST_S3_SECRET_ACCESS_KEY",
    "HTTP_TIMEOUT_SECONDS",
    "LIST_PAGE_SIZE",
    "VERSION_FILE",
    "WORKFLOW_NAMESPACE",
    *_AMBIENT_DECOYS,
    "AWS_SECURITY_TOKEN",
    "AWS_REGION",
    "AWS_PROFILE",
    "AWS_SHARED_CREDENTIALS_FILE",
    "AWS_CONFIG_FILE",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
)

# One cluster per transport, with the local entry deliberately in the middle
# so the transport is chosen per cluster and never by position.
_MIXED_CLUSTERS = json.dumps(
    [
        {
            "name": "edge-a",
            "base_url": "http://edge-a-proxy.example:8001",
            "namespace": "argo-a",
        },
        {"name": "home"},
        {"name": "edge-b", "base_url": "http://edge-b-proxy.example:8001"},
    ]
)

_S3_AND_AMBIENT_VALUES = (_S3_ACCESS_KEY, _S3_SECRET_KEY, *_AMBIENT_DECOYS.values())


def _set_env(monkeypatch, tmp_path, clusters_json):
    for key in _OWNED_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    version_file = tmp_path / "version"
    version_file.write_text("0.0.0-isolation\n")
    values = {
        "CLUSTERS_JSON": clusters_json,
        "DEST_S3_ENDPOINT": _S3_ENDPOINT,
        "DEST_S3_ACCESS_KEY_ID": _S3_ACCESS_KEY,
        "DEST_S3_SECRET_ACCESS_KEY": _S3_SECRET_KEY,
        "DEST_S3_BUCKET": _S3_BUCKET,
        "DEST_S3_REGION": "us-east-1",
        "DEST_S3_ADDRESSING_STYLE": "path",
        "HTTP_TIMEOUT_SECONDS": "13",
        "LIST_PAGE_SIZE": "500",
        "VERSION_FILE": str(version_file),
        **_AMBIENT_DECOYS,
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)


def _write_projected_service_account(tmp_path, monkeypatch):
    """Materialize the projected ServiceAccount the way kubelet presents it:
    a token file and a CA file read fresh for every request."""
    token_path = tmp_path / "token"
    token_path.write_text(_SA_TOKEN + "\n")
    ca_path = tmp_path / "ca.crt"
    ca_path.write_text(_SA_CA)
    monkeypatch.setattr(k8s_api, "_SA_TOKEN_PATH", str(token_path))
    monkeypatch.setattr(k8s_api, "_SA_CA_PATH", str(ca_path))
    return token_path, ca_path


def _workflow(host):
    return {
        "metadata": {
            "uid": f"uid-{host}",
            "name": f"wf-{host}",
            "namespace": "argo",
            "labels": {},
        },
        "spec": {},
        "status": {"phase": "Running"},
    }


class _RecordingTransport:
    """Answers one page per cluster URL and records every request's full
    shape — URL, params, and the exact keyword arguments that would reach
    the wire."""

    def __init__(self):
        self.calls = []

    def __call__(self, url, params=None, **kwargs):
        self.calls.append((url, dict(params or {}), kwargs))
        host = url.split("/", 3)[2]
        return SimpleNamespace(
            status_code=200,
            json=lambda: {"items": [_workflow(host)], "metadata": {}},
        )


class _FirstRunS3:
    """A destination bucket on its first run: the stored ledger is absent
    and every operation is recorded with the bucket it addressed."""

    def __init__(self):
        self.objects = {}
        self.addressed = []

    def get_object(self, Bucket, Key):
        self.addressed.append((Bucket, Key))
        raise ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "GetObject"
        )

    def put_object(self, Bucket, Key, Body, ContentType):
        self.addressed.append((Bucket, Key))
        self.objects[Key] = Body


def _run_mixed_cycle(monkeypatch, tmp_path):
    """Load the mixed fleet from the environment and publish one cycle
    against recorded transports. Returns `(transport, s3, ca_path)`.

    Only ``requests.get`` is replaced: config parsing, ServiceAccount
    reading, transport selection, listing, and publication all run for real.
    """
    _set_env(monkeypatch, tmp_path, _MIXED_CLUSTERS)
    _, ca_path = _write_projected_service_account(tmp_path, monkeypatch)
    transport = _RecordingTransport()
    monkeypatch.setattr(k8s_api.requests, "get", transport)
    s3 = _FirstRunS3()

    cfg = config.load()
    assert main._run_cycle(cfg, s3) is True
    return transport, s3, ca_path


def _assert_carries_none_of(request, values):
    rendered = repr(request)
    for value in values:
        assert value not in rendered, f"credential leaked into a request: {value}"


def test_local_cluster_requests_present_only_the_projected_service_account(
    monkeypatch, tmp_path
):
    transport, _, ca_path = _run_mixed_cycle(monkeypatch, tmp_path)

    local_calls = [
        call
        for call in transport.calls
        if call[0].startswith("https://kubernetes.default.svc")
    ]
    assert len(local_calls) == 1
    url, _, kwargs = local_calls[0]
    assert (
        url == "https://kubernetes.default.svc/apis/argoproj.io/v1alpha1/workflows"
    )
    # Exactly one credential is presented and it is the projected token,
    # verified against the projected CA: not the S3 keys, and nothing planted
    # in the wider environment. `headers`/`verify` are also the only auth
    # mechanisms on the call at all.
    assert kwargs["headers"] == {"Authorization": f"Bearer {_SA_TOKEN}"}
    assert kwargs["verify"] == str(ca_path)
    assert set(kwargs) == {"headers", "verify", "timeout"}
    _assert_carries_none_of(local_calls[0], _S3_AND_AMBIENT_VALUES)


def test_proxied_cluster_requests_carry_no_credential_from_any_source(
    monkeypatch, tmp_path
):
    transport, _, _ = _run_mixed_cycle(monkeypatch, tmp_path)

    proxied = {
        name: [
            call
            for call in transport.calls
            if call[0].startswith(f"http://{name}")
        ]
        for name in ("edge-a-proxy.example:8001", "edge-b-proxy.example:8001")
    }
    # Both proxy endpoints were polled — the one listed before the local
    # entry and the one after it.
    assert sorted(calls[0][0] for calls in proxied.values()) == [
        "http://edge-a-proxy.example:8001/apis/argoproj.io/v1alpha1/namespaces/argo-a/workflows",
        "http://edge-b-proxy.example:8001/apis/argoproj.io/v1alpha1/workflows",
    ]
    for calls in proxied.values():
        # The read-only proxy is reached credential-free: no Authorization,
        # no TLS material, and no credential value this run knows about —
        # including the local cluster's own projected ServiceAccount.
        assert calls[0][2] == {"timeout": 13}
        _assert_carries_none_of(
            calls[0], (_SA_TOKEN, _SA_CA, *_S3_AND_AMBIENT_VALUES)
        )


def test_cycle_s3_operations_address_only_the_configured_destination(
    monkeypatch, tmp_path
):
    transport, s3, _ = _run_mixed_cycle(monkeypatch, tmp_path)

    assert transport.calls  # the cycle collected every cluster
    # The only S3 identity used is the configured destination bucket; the
    # Kubernetes transports never touched it and no other bucket was tried.
    assert {bucket for bucket, _ in s3.addressed} == {_S3_BUCKET}


def test_s3_client_signs_with_only_the_loaded_dest_s3_settings(monkeypatch, tmp_path):
    _set_env(monkeypatch, tmp_path, _MIXED_CLUSTERS)
    cfg = config.load()
    client = s3io.client(cfg.dest)

    # The credentials the client actually signs every request with, read
    # through botocore's signer: exactly the DEST_S3_* values, with every
    # planted ambient AWS_* fallback — credentials, region, and endpoint —
    # losing to the explicit configuration.
    credentials = client._request_signer._credentials.get_frozen_credentials()
    assert credentials.access_key == _S3_ACCESS_KEY
    assert credentials.secret_key == _S3_SECRET_KEY
    assert credentials.token is None
    assert client.meta.endpoint_url == _S3_ENDPOINT
    assert client.meta.region_name == "us-east-1"
    # And nothing from the Kubernetes side crosses into the destination
    # client's signing material or endpoint.
    assert _SA_TOKEN not in client.meta.endpoint_url
    assert _SA_TOKEN not in (credentials.access_key, credentials.secret_key)
