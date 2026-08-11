import json
import logging
import signal
import sys
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import config, ledger, parquet_io, s3io, workflows

log = logging.getLogger(__name__)

_ready = threading.Event()


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200 if _ready.is_set() else 503)
            self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):
        pass


def _serve_health(port: int):
    server = ThreadingHTTPServer(("0.0.0.0", port), _HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log.info("health server listening on :%d/health", port)


def _now() -> str:
    # Explicit "Z" suffix, not a naive isoformat() -- a timestamp with no
    # UTC offset gets misread as local time by browsers.
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _run_cycle(cfg: config.Config, s3) -> bool:
    generated_at = _now()

    rows, cluster_stats = workflows.fetch_workflows(
        cfg.clusters, cfg.namespace, cfg.http_timeout_seconds, cfg.page_size, generated_at
    )

    if not any(c["ok"] for c in cluster_stats):
        # Nothing answered. Writing now would replace a good snapshot with an
        # empty one and stamp meta.json fresh -- i.e. it would report an
        # outage as "zero workflows, up to date". Leaving every object
        # untouched instead makes meta.json's generated_at go stale, which is
        # the signal a consumer can actually act on.
        log.error("no cluster answered this cycle; leaving stored objects untouched")
        return False

    s3io.upload_bytes(
        s3, cfg.dest.bucket, f"{cfg.dest_prefix}/workflows.parquet",
        parquet_io.table_to_parquet_bytes(rows, parquet_io.WORKFLOWS_SCHEMA),
        "application/octet-stream",
    )

    runs_key = f"{cfg.dest_prefix}/runs.parquet"
    stored = parquet_io.parquet_bytes_to_table(
        s3io.download_bytes(s3, cfg.dest.bucket, runs_key), parquet_io.RUNS_SCHEMA
    )
    merged = ledger.merge(stored.to_pylist(), rows, generated_at, cfg.run_retention_days)
    s3io.upload_bytes(
        s3, cfg.dest.bucket, runs_key,
        parquet_io.table_to_parquet_bytes(merged, parquet_io.RUNS_SCHEMA),
        "application/octet-stream",
    )

    meta = json.dumps(
        {
            "version": cfg.version,
            "generated_at": generated_at,
            "poll_interval_seconds": cfg.poll_interval_seconds,
            "run_retention_days": cfg.run_retention_days,
            "clusters": cluster_stats,
            "workflows": len(rows),
            "runs": len(merged),
        }
    ).encode()
    s3io.upload_bytes(s3, cfg.dest.bucket, f"{cfg.dest_prefix}/meta.json", meta, "application/json")
    return True


def main():
    try:
        cfg = config.load()
    except config.ConfigError as e:
        print(f"config error: {e}", file=sys.stderr)
        sys.exit(1)

    logging.basicConfig(level=cfg.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    log.info(
        "polling %d cluster(s) every %ds, writing to s3://%s/%s",
        len(cfg.clusters), cfg.poll_interval_seconds, cfg.dest.bucket, cfg.dest_prefix,
    )

    s3 = s3io.client(cfg.dest)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    _serve_health(cfg.health_port)

    while not stop.is_set():
        try:
            if _run_cycle(cfg, s3):
                _ready.set()
        except Exception:
            log.exception("cycle failed, will retry next interval")
        stop.wait(cfg.poll_interval_seconds)

    log.info("stopped")


if __name__ == "__main__":
    main()
