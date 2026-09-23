from pathlib import Path

import pytest

from src.config import ConfigError, load

_REQUIRED = {
    "DEST_S3_ENDPOINT": "https://s3.example.com",
    "DEST_S3_ACCESS_KEY_ID": "key",
    "DEST_S3_SECRET_ACCESS_KEY": "secret",
    "DEST_S3_BUCKET": "bucket",
}

_CONFIG_KEYS = {
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
}

_NUMERIC = (
    "POLL_INTERVAL_SECONDS",
    "RUN_RETENTION_DAYS",
    "HTTP_TIMEOUT_SECONDS",
    "LIST_PAGE_SIZE",
    "HEALTH_PORT",
)


def _env(monkeypatch, clusters_json, **extra):
    for key in _CONFIG_KEYS:
        monkeypatch.delenv(key, raising=False)
    values = {**_REQUIRED, "CLUSTERS_JSON": clusters_json, **extra}
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("VERSION_FILE", values.get("VERSION_FILE", "VERSION"))


def test_remote_only_fleet_is_valid(monkeypatch):
    _env(monkeypatch, '[{"name": "ci", "base_url": "http://proxy.example:8001"}]')
    cfg = load()
    assert [c.name for c in cfg.clusters] == ["ci"]
    assert cfg.namespace == ""
    assert cfg.dest_prefix == "argo/data"


def test_single_local_cluster_is_valid(monkeypatch):
    _env(monkeypatch, '[{"name": "local"}]')
    cfg = load()
    assert len(cfg.clusters) == 1
    assert cfg.clusters[0].base_url is None


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


@pytest.mark.parametrize("raw", ["{}", "[]", "null"])
def test_clusters_must_be_a_non_empty_array(monkeypatch, raw):
    _env(monkeypatch, raw)
    with pytest.raises(ConfigError, match="non-empty JSON array"):
        load()


def test_cluster_entries_must_be_objects(monkeypatch):
    _env(monkeypatch, '["ci"]')
    with pytest.raises(ConfigError, match="not an object"):
        load()


@pytest.mark.parametrize("name", [*_REQUIRED, "CLUSTERS_JSON"])
@pytest.mark.parametrize("value", ["", "  "])
def test_missing_required_variables_fail_fast(monkeypatch, name, value):
    _env(monkeypatch, '[{"name": "ci", "base_url": "http://p:8001"}]')
    monkeypatch.setenv(name, value)
    with pytest.raises(ConfigError, match=name):
        load()


def test_s3_defaults_are_applied(monkeypatch):
    _env(monkeypatch, '[{"name": "ci", "base_url": "http://p:8001"}]')
    cfg = load()
    assert cfg.dest.addressing_style == "virtual"
    assert cfg.dest.region == "us-east-1"
    assert cfg.dest_prefix == "argo/data"


def test_s3_addressing_style_can_be_overridden(monkeypatch):
    _env(
        monkeypatch,
        '[{"name": "ci", "base_url": "http://p:8001"}]',
        DEST_S3_ADDRESSING_STYLE="path",
    )
    assert load().dest.addressing_style == "path"


def test_trailing_slash_is_stripped_from_the_prefix(monkeypatch):
    _env(monkeypatch, '[{"name": "ci", "base_url": "http://p:8001"}]', DEST_S3_PREFIX="argo/data/")
    assert load().dest_prefix == "argo/data"


def test_per_cluster_namespace_override_is_preserved(monkeypatch):
    _env(
        monkeypatch,
        '[{"name": "ci", "namespace": "team-a"}, {"name": "staging", "base_url": "http://p:8001"}]',
        WORKFLOW_NAMESPACE="global",
    )
    cfg = load()
    assert cfg.namespace == "global"
    assert [(c.name, c.namespace) for c in cfg.clusters] == [
        ("ci", "team-a"),
        ("staging", None),
    ]


@pytest.mark.parametrize("name", _NUMERIC)
@pytest.mark.parametrize("value", ["0", "-1"])
def test_numeric_settings_must_be_greater_than_zero(monkeypatch, name, value):
    _env(monkeypatch, '[{"name": "ci", "base_url": "http://p:8001"}]', **{name: value})
    with pytest.raises(ConfigError, match=f"{name} must be greater than zero"):
        load()


@pytest.mark.parametrize("name", _NUMERIC)
def test_numeric_settings_must_be_integers(monkeypatch, name):
    _env(monkeypatch, '[{"name": "ci", "base_url": "http://p:8001"}]', **{name: "1.5"})
    with pytest.raises(ConfigError, match=f"{name} must be an integer"):
        load()


def test_default_version_file_is_read(monkeypatch):
    _env(monkeypatch, '[{"name": "ci", "base_url": "http://p:8001"}]')
    monkeypatch.delenv("VERSION_FILE")
    assert load().version == Path("VERSION").read_text().strip()


def test_version_file_contents_are_read_and_stripped(monkeypatch, tmp_path):
    version_file = tmp_path / "version"
    version_file.write_text("  1.2.3\n")
    _env(monkeypatch, '[{"name": "ci", "base_url": "http://p:8001"}]', VERSION_FILE=str(version_file))
    assert load().version == "1.2.3"


def test_missing_version_file_falls_back_to_unknown(monkeypatch, tmp_path):
    version_file = tmp_path / "missing-version"
    _env(monkeypatch, '[{"name": "ci", "base_url": "http://p:8001"}]', VERSION_FILE=str(version_file))
    assert load().version == "unknown"
