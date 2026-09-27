"""Integration coverage for configuration values after ``config.load``.

The parser tests establish that environment variables are accepted. These
tests follow the resulting ``Config`` into the HTTP client, publication
metadata, polling loop, health server, logger, and S3 client.
"""

import http.client
import json
import logging
import socket
from types import SimpleNamespace

from botocore.exceptions import ClientError

from src import config, k8s_api, main


_ENV_KEYS = (
    "CLUSTERS_JSON",
    "DEST_S3_ACCESS_KEY_ID",
    "DEST_S3_ADDRESSING_STYLE",
    "DEST_S3_BUCKET",
    "DEST_S3_ENDPOINT",
    "DEST_S3_PREFIX",
    "DEST_S3_REGION",
    "DEST_S3_SECRET_ACCESS_KEY",
    "HEALTH_PORT",
    "HTTP_TIMEOUT_SECONDS",
    "LIST_PAGE_SIZE",
    "LOG_LEVEL",
    "POLL_INTERVAL_SECONDS",
    "RUN_RETENTION_DAYS",
    "VERSION_FILE",
    "WORKFLOW_NAMESPACE",
)


_DEFAULTS = {
    "CLUSTERS_JSON": '[{"name": "ci", "base_url": "http://proxy.example:8001"}]',
    "DEST_S3_ENDPOINT": "http://s3.example",
    "DEST_S3_ACCESS_KEY_ID": "access",
    "DEST_S3_SECRET_ACCESS_KEY": "secret",
    "DEST_S3_BUCKET": "bucket",
    "POLL_INTERVAL_SECONDS": "45",
    "RUN_RETENTION_DAYS": "9",
    "HTTP_TIMEOUT_SECONDS": "13",
    "LIST_PAGE_SIZE": "2",
    "HEALTH_PORT": "8080",
    "LOG_LEVEL": "INFO",
    "DEST_S3_REGION": "us-east-1",
    "DEST_S3_ADDRESSING_STYLE": "virtual",
}


def _set_env(monkeypatch, tmp_path, **overrides):
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    version_file = tmp_path / "version"
    version_file.write_text("7.4.1-test\n")
    values = {**_DEFAULTS, "VERSION_FILE": str(version_file), **overrides}
    for key, value in values.items():
        monkeypatch.setenv(key, value)


def _workflow(uid, name):
    return {
        "metadata": {"uid": uid, "name": name, "namespace": "argo", "labels": {}},
        "spec": {},
        "status": {"phase": "Running"},
    }


class _FirstRunS3:
    def __init__(self):
        self.objects = {}

    def get_object(self, Bucket, Key):
        raise ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "GetObject"
        )

    def put_object(self, Bucket, Key, Body, ContentType):
        self.objects[Key] = Body


def test_loaded_http_and_version_settings_reach_a_published_generation(
    monkeypatch, tmp_path
):
    version_file = tmp_path / "release-version"
    version_file.write_text("9.9.9-rc.7\n")
    _set_env(monkeypatch, tmp_path, VERSION_FILE=str(version_file))
    cfg = config.load()

    pages = {
        None: {
            "items": [_workflow("wf-1", "one"), _workflow("wf-2", "two")],
            "metadata": {"continue": "page-2"},
        },
        "page-2": {"items": [_workflow("wf-3", "three")], "metadata": {}},
    }
    calls = []

    def fake_get(url, params=None, **kwargs):
        params = dict(params or {})
        calls.append((url, params, kwargs))
        return SimpleNamespace(
            status_code=200,
            json=lambda: pages[params.get("continue")],
        )

    monkeypatch.setattr(k8s_api.requests, "get", fake_get)
    s3 = _FirstRunS3()

    assert main._run_cycle(cfg, s3) is True

    assert [url for url, _, _ in calls] == [
        "http://proxy.example:8001/apis/argoproj.io/v1alpha1/workflows",
        "http://proxy.example:8001/apis/argoproj.io/v1alpha1/workflows",
    ]
    assert [params for _, params, _ in calls] == [
        {"limit": 2},
        {"limit": 2, "continue": "page-2"},
    ]
    assert [kwargs["timeout"] for _, _, kwargs in calls] == [13, 13]

    meta = json.loads(s3.objects["argo/data/meta.json"])
    assert meta["version"] == "9.9.9-rc.7"
    assert meta["poll_interval_seconds"] == 45
    assert meta["workflows"] == 3


def test_loaded_poll_interval_controls_waiting_and_health_staleness(
    monkeypatch, tmp_path
):
    _set_env(monkeypatch, tmp_path)
    cfg = config.load()

    waits = []

    class Stop:
        def __init__(self):
            self.stopped = False

        def is_set(self):
            return self.stopped

        def wait(self, interval):
            waits.append(interval)
            self.stopped = True
            return True

    monkeypatch.setattr(main, "_run_cycle", lambda _cfg, _s3: True)
    main._ready.clear()
    try:
        main._run_poll_loop(cfg, object(), Stop())
    finally:
        main._ready.clear()

    assert waits == [45]
    # The health contract uses two polling intervals as its freshness window.
    state = main._HealthState(cfg.poll_interval_seconds)
    assert state.poll_interval_seconds == 45
    assert state.stale_after_seconds == 90


def test_loaded_log_health_and_s3_settings_reach_startup_runtime(monkeypatch, tmp_path):
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()

    _set_env(
        monkeypatch,
        tmp_path,
        LOG_LEVEL="WARNING",
        HEALTH_PORT=str(port),
        DEST_S3_REGION="eu-central-1",
        DEST_S3_ADDRESSING_STYLE="path",
    )

    root = logging.getLogger()
    previous_level = root.level
    root.setLevel(logging.DEBUG)
    observed = {}

    def fake_signal(_signum, _handler):
        return None

    def fake_loop(cfg, s3, stop, health_state):
        observed["cfg"] = cfg
        observed["s3"] = s3
        observed["health_state"] = health_state
        observed["root_level"] = root.level

        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
        try:
            connection.request("GET", "/health")
            response = connection.getresponse()
            observed["health_response"] = (response.status, json.loads(response.read()))
        finally:
            connection.close()
        stop.set()

    monkeypatch.setattr(main.signal, "signal", fake_signal)
    monkeypatch.setattr(main, "_run_poll_loop", fake_loop)

    try:
        main.main()
    finally:
        root.setLevel(previous_level)

    assert observed["root_level"] == logging.WARNING
    assert observed["health_response"] == (
        503,
        {"status": "starting", "last_success_at": None},
    )
    assert observed["health_state"].stale_after_seconds == 90

    s3 = observed["s3"]
    assert s3.meta.region_name == "eu-central-1"
    # botocore may add its own defaults, but the configured addressing style
    # must remain the value used for requests.
    assert s3.meta.config.s3["addressing_style"] == "path"
