import json
import logging
import signal
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

from . import config, ledger, meta_schema, parquet_io, s3io, workflows

log = logging.getLogger(__name__)

HEALTH_STALE_AFTER_MULTIPLIER = 2
_ready = threading.Event()


class _HealthState:
    def __init__(
        self,
        poll_interval_seconds,
        stale_after_seconds=None,
        monotonic=None,
        wall_clock=None,
    ):
        self.poll_interval_seconds = poll_interval_seconds
        self.stale_after_seconds = (
            poll_interval_seconds * HEALTH_STALE_AFTER_MULTIPLIER
            if stale_after_seconds is None
            else stale_after_seconds
        )
        self._monotonic = time.monotonic if monotonic is None else monotonic
        self._wall_clock = time.time if wall_clock is None else wall_clock
        self._lock = threading.Lock()
        self._last_success = None
        self._last_success_wall = None

    def reset(self):
        with self._lock:
            self._last_success = None
            self._last_success_wall = None
        _ready.clear()

    def record_success(self):
        now = self._monotonic()
        wall_now = self._wall_clock()
        with self._lock:
            self._last_success = now
            self._last_success_wall = wall_now
        _ready.set()

    def snapshot(self):
        now = self._monotonic()
        with self._lock:
            last_success = self._last_success
            last_success_wall = self._last_success_wall

        if last_success is None:
            _ready.clear()
            return 503, {"status": "starting", "last_success_at": None}

        age = max(0.0, now - last_success)
        if age >= self.stale_after_seconds:
            _ready.clear()
            return 503, {
                "status": "stale",
                "last_success_at": _timestamp(last_success_wall),
                "age_seconds": round(age, 3),
            }
        return 200, {
            "status": "ok",
            "last_success_at": _timestamp(last_success_wall),
            "age_seconds": round(age, 3),
        }


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if urlsplit(self.path).path != "/health":
            self._write_json(404, {"status": "not_found"})
            return

        state = getattr(self.server, "health_state", None)
        if state is None:
            code = 200 if _ready.is_set() else 503
            payload = {"status": "ok" if code == 200 else "starting"}
        else:
            code, payload = state.snapshot()
        self._write_json(code, payload)

    def _write_json(self, code, payload):
        body = json.dumps(payload, separators=(",", ":")).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass


def _serve_health(port: int, health_state=None):
    server = ThreadingHTTPServer(("0.0.0.0", port), _HealthHandler)
    if health_state is not None:
        server.health_state = health_state
    server.daemon_threads = True
    started = threading.Event()

    def serve():
        started.set()
        server.serve_forever()

    thread = threading.Thread(
        target=serve,
        name="health-server",
        daemon=True,
    )
    server.thread = thread

    try:
        thread.start()
        if not started.wait(timeout=1):
            raise RuntimeError("health server did not start")
    except Exception:
        server.server_close()
        raise
    log.info("health server listening on :%d/health", server.server_address[1])
    return server


def _stop_health(server):
    if server is None:
        return
    thread = getattr(server, "thread", None)
    try:
        if thread is None or thread.is_alive():
            server.shutdown()
    finally:
        try:
            server.server_close()
        finally:
            if thread is not None:
                thread.join()


def _timestamp(value):
    if value is None:
        return None
    return datetime.fromtimestamp(value, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
    meta = {
        "version": cfg.version,
        "generated_at": generated_at,
        "generation_id": generation_id,
        "poll_interval_seconds": cfg.poll_interval_seconds,
        "run_retention_days": cfg.run_retention_days,
        "clusters": cluster_stats,
        "workflows": len(rows),
        "runs": len(merged),
    }
    # The sidecar is the commit marker: consumers parse it before either
    # Parquet file, so a shape regression would publish a generation that
    # misdescribes itself. This runs in the compute phase -- a refusal here
    # has written nothing, and the next cycle simply retries.
    meta_schema.validate(meta)
    meta_payload = json.dumps(meta).encode()

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


def _run_poll_loop(cfg, s3, stop, health_state=None):
    if health_state is None:
        health_state = _HealthState(cfg.poll_interval_seconds)
    while not stop.is_set():
        try:
            if _run_cycle(cfg, s3):
                health_state.record_success()
                log.info("cycle completed")
            else:
                log.warning("cycle did not complete, will retry next interval")
        except Exception:
            if stop.is_set():
                log.exception("cycle failed during shutdown")
            else:
                log.exception("cycle failed, will retry next interval")
        stop.wait(cfg.poll_interval_seconds)


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
    previous_handlers = {}

    def request_shutdown(signum, _frame):
        log.info("received %s, shutting down", signal.Signals(signum).name)
        stop.set()

    for signum in (signal.SIGTERM, signal.SIGINT):
        previous_handlers[signum] = signal.signal(signum, request_shutdown)

    health_state = _HealthState(cfg.poll_interval_seconds)
    health_state.reset()
    health_server = None
    try:
        health_server = _serve_health(cfg.health_port, health_state)
        _run_poll_loop(cfg, s3, stop, health_state)
    finally:
        stop.set()
        try:
            _stop_health(health_server)
        finally:
            for signum, handler in previous_handlers.items():
                signal.signal(signum, handler)
            log.info("stopped")


if __name__ == "__main__":
    main()
