import json
import os
from dataclasses import dataclass


class ConfigError(Exception):
    pass


def _require(name):
    value = os.environ.get(name, "").strip()
    if not value:
        raise ConfigError(f"missing required env var: {name}")
    return value


def _optional(name, default):
    value = os.environ.get(name, "").strip()
    return value if value else default


@dataclass(frozen=True)
class Cluster:
    name: str
    base_url: str | None = None  # None means "local" — use in-cluster config
    namespace: str | None = None  # None means "inherit WORKFLOW_NAMESPACE"


@dataclass(frozen=True)
class S3Endpoint:
    endpoint_url: str
    access_key_id: str
    secret_access_key: str
    bucket: str
    addressing_style: str
    region: str


@dataclass(frozen=True)
class Config:
    clusters: list
    namespace: str
    dest: S3Endpoint
    dest_prefix: str
    version: str
    poll_interval_seconds: int
    run_retention_days: int
    http_timeout_seconds: int
    page_size: int
    health_port: int
    log_level: str


def _parse_clusters(raw: str):
    try:
        items = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ConfigError(f"CLUSTERS_JSON is not valid JSON: {e}")

    if not isinstance(items, list) or not items:
        raise ConfigError("CLUSTERS_JSON must be a non-empty JSON array")

    clusters = []
    for item in items:
        try:
            clusters.append(
                Cluster(
                    name=item["name"],
                    base_url=item.get("base_url"),
                    namespace=item.get("namespace"),
                )
            )
        except KeyError as e:
            raise ConfigError(f"CLUSTERS_JSON entry missing required key {e}: {item}")
        except TypeError:
            raise ConfigError(f"CLUSTERS_JSON entry is not an object: {item}")

    names = [c.name for c in clusters]
    if len(set(names)) != len(names):
        raise ConfigError(f"CLUSTERS_JSON has duplicate cluster names: {names}")

    # Zero local entries is legitimate and is in fact the common shape: this
    # exporter usually runs somewhere other than the cluster it watches, and
    # reaches every target over a proxy. More than one is not — "local" means
    # this pod's own ServiceAccount, and there is only one of those.
    local = [c for c in clusters if c.base_url is None]
    if len(local) > 1:
        raise ConfigError(
            f"CLUSTERS_JSON has {len(local)} entries with no base_url; at most one "
            f"(the cluster this pod runs in) may omit it: {[c.name for c in local]}"
        )
    return clusters


def _read_version(version_file):
    try:
        with open(version_file) as f:
            return f.read().strip()
    except OSError:
        return "unknown"


def _positive_int(name, default):
    raw = _optional(name, default)
    try:
        value = int(raw)
    except ValueError:
        raise ConfigError(f"{name} must be an integer, got {raw!r}")
    if value <= 0:
        raise ConfigError(f"{name} must be greater than zero, got {value}")
    return value


def load() -> Config:
    clusters = _parse_clusters(_require("CLUSTERS_JSON"))

    dest = S3Endpoint(
        endpoint_url=_require("DEST_S3_ENDPOINT"),
        access_key_id=_require("DEST_S3_ACCESS_KEY_ID"),
        secret_access_key=_require("DEST_S3_SECRET_ACCESS_KEY"),
        bucket=_require("DEST_S3_BUCKET"),
        addressing_style=_optional("DEST_S3_ADDRESSING_STYLE", "virtual"),
        region=_optional("DEST_S3_REGION", "us-east-1"),
    )

    return Config(
        clusters=clusters,
        # "" means every namespace — the cluster-scoped list endpoint. A
        # per-cluster `namespace` overrides this for that cluster only.
        namespace=_optional("WORKFLOW_NAMESPACE", ""),
        dest=dest,
        dest_prefix=_optional("DEST_S3_PREFIX", "argo/data").rstrip("/"),
        version=_read_version(_optional("VERSION_FILE", "VERSION")),
        poll_interval_seconds=_positive_int("POLL_INTERVAL_SECONDS", "300"),
        run_retention_days=_positive_int("RUN_RETENTION_DAYS", "7"),
        http_timeout_seconds=_positive_int("HTTP_TIMEOUT_SECONDS", "10"),
        page_size=_positive_int("LIST_PAGE_SIZE", "500"),
        health_port=_positive_int("HEALTH_PORT", "8080"),
        log_level=_optional("LOG_LEVEL", "INFO"),
    )
