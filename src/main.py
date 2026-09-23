import json
import logging
import signal
import sys
import threading
import uuid
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
    """One poll-publish cycle, in three phases. See
    docs/notes/atomic-publication.md for the full write-ordering contract.

    S3 has no multi-object write, so the three objects of a generation cannot
    be published atomically. The ordering here is what makes a torn
    publication harmless instead of silent:

    1. Read phase -- fetch every cluster and download the stored ledger
       before writing anything. A failure here leaves the stored generation
       completely untouched.
    2. Compute phase -- merge and encode all three payloads in memory, so an
       encode failure also cannot leave a half-published generation.
    3. Publish phase -- upload workflows.parquet, then runs.parquet, then
       meta.json. meta.json is last because it is the commit marker: it is
       the object consumers read first, and every payload it describes is
       already in place when it lands. All three carry the same
       `generation_id`, so a consumer can detect the torn set a mid-phase
       failure leaves behind and hold its previous good generation.
    """
    generated_at = _now()
    # Second-resolution timestamps collide if two cycles ever run that close
    # together; the random suffix makes each publication's id its own.
    generation_id = f"{generated_at}-{uuid.uuid4().hex[:12]}"

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

    runs_key = f"{cfg.dest_prefix}/runs.parquet"
    stored = parquet_io.parquet_bytes_to_table(
        s3io.download_bytes(s3, cfg.dest.bucket, runs_key), parquet_io.RUNS_SCHEMA
    )
    merged = ledger.merge(stored.to_pylist(), rows, generated_at, cfg.run_retention_days)

    workflows_payload = parquet_io.table_to_parquet_bytes(
        rows, parquet_io.WORKFLOWS_SCHEMA, generation_id
    )
    runs_payload = parquet_io.table_to_parquet_bytes(
        merged, parquet_io.RUNS_SCHEMA, generation_id
    )
    meta_payload = json.dumps(
        {
            "version": cfg.version,
            "generated_at": generated_at,
            "generation_id": generation_id,
            "poll_interval_seconds": cfg.poll_interval_seconds,
            "run_retention_days": cfg.run_retention_days,
            "clusters": cluster_stats,
            "workflows": len(rows),
            "runs": len(merged),
        }
    ).encode()

    published = []
    uploads = (
        (f"{cfg.dest_prefix}/workflows.parquet", workflows_payload, "application/octet-stream"),
        (runs_key, runs_payload, "application/octet-stream"),
        (f"{cfg.dest_prefix}/meta.json", meta_payload, "application/json"),
    )
    try:
        for key, payload, content_type in uploads:
            s3io.upload_bytes(s3, cfg.dest.bucket, key, payload, content_type)
            published.append(key)
    except Exception:
        log.exception(
            "publication of generation %s aborted after writing [%s]; objects still on "
            "the old generation: [%s] -- the stored set is torn and detectable by its "
            "mismatched generation_ids until the next successful cycle republishes "
            "all three",
            generation_id,
            ", ".join(published) or "none",
            ", ".join(key for key, _, _ in uploads if key not in published),
        )
        raise
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
