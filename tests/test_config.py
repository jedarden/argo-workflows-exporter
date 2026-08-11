import pytest

from src.config import ConfigError, load

_REQUIRED = {
    "DEST_S3_ENDPOINT": "https://s3.example.com",
    "DEST_S3_ACCESS_KEY_ID": "key",
    "DEST_S3_SECRET_ACCESS_KEY": "secret",
    "DEST_S3_BUCKET": "bucket",
}


def _env(monkeypatch, clusters_json, **extra):
    for key in list(_REQUIRED) + ["CLUSTERS_JSON", "WORKFLOW_NAMESPACE", "POLL_INTERVAL_SECONDS",
                                  "DEST_S3_PREFIX", "RUN_RETENTION_DAYS"]:
        monkeypatch.delenv(key, raising=False)
    for key, value in {**_REQUIRED, "CLUSTERS_JSON": clusters_json, **extra}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("VERSION_FILE", "VERSION")


def test_remote_only_fleet_is_valid(monkeypatch):
    """The usual shape: the exporter runs somewhere other than the cluster it
    watches and reaches every target over a proxy."""
    _env(monkeypatch, '[{"name": "ci", "base_url": "http://proxy.example:8001"}]')
    cfg = load()
    assert [c.name for c in cfg.clusters] == ["ci"]
    assert cfg.namespace == ""
    assert cfg.dest_prefix == "argo/data"


def test_at_most_one_local_cluster(monkeypatch):
    _env(monkeypatch, '[{"name": "a"}, {"name": "b"}]')
    with pytest.raises(ConfigError, match="at most one"):
        load()


def test_duplicate_cluster_names_are_rejected(monkeypatch):
    _env(monkeypatch, '[{"name": "ci"}, {"name": "ci", "base_url": "http://p:8001"}]')
    with pytest.raises(ConfigError, match="duplicate"):
        load()


def test_missing_name_is_reported_with_the_offending_entry(monkeypatch):
    _env(monkeypatch, '[{"base_url": "http://proxy.example:8001"}]')
    with pytest.raises(ConfigError, match="missing required key"):
        load()


def test_malformed_json_is_reported_as_such(monkeypatch):
    _env(monkeypatch, "not json")
    with pytest.raises(ConfigError, match="not valid JSON"):
        load()


def test_missing_destination_fails_fast(monkeypatch):
    _env(monkeypatch, '[{"name": "ci", "base_url": "http://p:8001"}]')
    monkeypatch.delenv("DEST_S3_BUCKET")
    with pytest.raises(ConfigError, match="DEST_S3_BUCKET"):
        load()


def test_non_positive_interval_is_rejected(monkeypatch):
    _env(monkeypatch, '[{"name": "ci", "base_url": "http://p:8001"}]', POLL_INTERVAL_SECONDS="0")
    with pytest.raises(ConfigError, match="greater than zero"):
        load()


def test_trailing_slash_is_stripped_from_the_prefix(monkeypatch):
    _env(monkeypatch, '[{"name": "ci", "base_url": "http://p:8001"}]', DEST_S3_PREFIX="argo/data/")
    assert load().dest_prefix == "argo/data"
