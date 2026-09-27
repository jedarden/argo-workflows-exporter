import json
import os
from dataclasses import dataclass


class ConfigError(Exception):
    pass


_LOG_LEVEL_ALIASES = {
    "CRITICAL": "CRITICAL",
    "ERROR": "ERROR",
    "FATAL": "CRITICAL",
    "WARN": "WARNING",
    "WARNING": "WARNING",
    "INFO": "INFO",
    "DEBUG": "DEBUG",
    "NOTSET": "NOTSET",
}

_S3_ADDRESSING_STYLES = ("auto", "virtual", "path")
_DEFAULT_DEST_S3_PREFIX = "argo/data"
_DEFAULT_RUN_RETENTION_DAYS = 7


def _require(name):
    value = os.environ.get(name, "").strip()
    if not value:
        raise ConfigError(f"missing required env var: {name}")
    return value


def _optional(name, default):
    value = os.environ.get(name, "").strip()
    return value if value else default


def normalize_s3_prefix(prefix: str) -> str:
    """Return the canonical boundary-safe form of an S3 key prefix.

    S3 object keys are not filesystem paths: an empty prefix means the bucket
    root, and slashes at either boundary are only separators supplied by the
    configuration. Strip those boundary slashes so callers can add exactly
    one separator when joining an object basename.
    """
    return prefix.strip().strip("/")


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

    if not isinstance(items, list):
        raise ConfigError("CLUSTERS_JSON must be a JSON array")
    if not items:
        raise ConfigError(
            "CLUSTERS_JSON must be a non-empty JSON array; at least one cluster is required"
        )

    clusters = []
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            raise ConfigError(
                f"CLUSTERS_JSON entry {index} is not an object: {item}"
            )

        if "name" not in item:
            raise ConfigError(
                f"CLUSTERS_JSON entry missing required key 'name': {item}"
            )
        name = item["name"]
        if not isinstance(name, str) or not name:
            raise ConfigError(
                f"CLUSTERS_JSON entry {index} name must be a non-empty string: {item}"
            )

        optional_fields = {
            "base_url": item.get("base_url"),
            "namespace": item.get("namespace"),
        }
        for field, value in optional_fields.items():
            if value is not None and not isinstance(value, str):
                raise ConfigError(
                    f"CLUSTERS_JSON entry {index} {field} must be a string or null: {item}"
                )

        clusters.append(
            Cluster(
                name=name,
                base_url=optional_fields["base_url"],
                namespace=optional_fields["namespace"],
            )
        )

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
        with open(version_file, encoding="utf-8") as f:
            version = f.read().strip()
    except (OSError, UnicodeError):
        return "unknown"
    return version or "unknown"


def _positive_int(name, default):
    raw = _optional(name, default)
    try:
        value = int(raw)
    except ValueError:
        raise ConfigError(f"{name} must be an integer, got {raw!r}")
    if value <= 0:
        raise ConfigError(f"{name} must be greater than zero, got {value}")
    return value


def _run_retention_days():
    """Load the optional retention window, whose minimum is one day."""
    return _positive_int("RUN_RETENTION_DAYS", str(_DEFAULT_RUN_RETENTION_DAYS))


def _log_level():
    raw = _optional("LOG_LEVEL", "INFO").upper()
    try:
        return _LOG_LEVEL_ALIASES[raw]
    except KeyError:
        allowed = ", ".join(sorted(_LOG_LEVEL_ALIASES))
        raise ConfigError(f"LOG_LEVEL must be one of {allowed}, got {raw!r}")


def _s3_addressing_style():
    raw = _optional("DEST_S3_ADDRESSING_STYLE", "virtual")
    if raw not in _S3_ADDRESSING_STYLES:
        allowed = ", ".join(_S3_ADDRESSING_STYLES)
        raise ConfigError(
            f"DEST_S3_ADDRESSING_STYLE must be one of {allowed}, got {raw!r}"
        )
    return raw


def load() -> Config:
    clusters = _parse_clusters(_require("CLUSTERS_JSON"))

    dest = S3Endpoint(
        endpoint_url=_require("DEST_S3_ENDPOINT"),
        access_key_id=_require("DEST_S3_ACCESS_KEY_ID"),
        secret_access_key=_require("DEST_S3_SECRET_ACCESS_KEY"),
        bucket=_require("DEST_S3_BUCKET"),
        addressing_style=_s3_addressing_style(),
        region=_optional("DEST_S3_REGION", "us-east-1"),
    )

    return Config(
        clusters=clusters,
        # "" means every namespace — the cluster-scoped list endpoint. A
        # per-cluster `namespace` overrides this for that cluster only.
        namespace=_optional("WORKFLOW_NAMESPACE", ""),
        dest=dest,
        # An omitted variable gets the documented default. An explicitly
        # empty variable is meaningful: it selects the bucket root.
        dest_prefix=normalize_s3_prefix(
            os.environ.get("DEST_S3_PREFIX", _DEFAULT_DEST_S3_PREFIX)
        ),
        version=_read_version(_optional("VERSION_FILE", "VERSION")),
        poll_interval_seconds=_positive_int("POLL_INTERVAL_SECONDS", "300"),
        run_retention_days=_run_retention_days(),
        http_timeout_seconds=_positive_int("HTTP_TIMEOUT_SECONDS", "10"),
        page_size=_positive_int("LIST_PAGE_SIZE", "500"),
        health_port=_positive_int("HEALTH_PORT", "8080"),
        log_level=_log_level(),
    )
