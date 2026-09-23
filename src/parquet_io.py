import io

import pyarrow as pa
import pyarrow.parquet as pq

# Columns shared by both outputs. The snapshot adds `observed_at`; the run
# ledger adds `first_seen_at` / `last_seen_at` instead.
_WORKFLOW_FIELDS = [
    ("uid", pa.string()),
    ("cluster", pa.string()),
    ("namespace", pa.string()),
    ("name", pa.string()),
    ("template", pa.string()),
    ("template_scope", pa.string()),
    ("trigger_kind", pa.string()),
    ("trigger_name", pa.string()),
    ("phase", pa.string()),
    ("message", pa.string()),
    ("progress", pa.string()),
    ("created_at", pa.string()),
    ("started_at", pa.string()),
    ("finished_at", pa.string()),
    ("duration_seconds", pa.int64()),
    ("resources_duration_cpu", pa.int64()),
    ("resources_duration_memory", pa.int64()),
    ("failed_step", pa.string()),
    ("failed_step_message", pa.string()),
    # Phase 3a: the failure message reduced to something groupable. Both are
    # derived from `failed_step_message` (falling back to `message`), which is
    # kept unchanged alongside them.
    ("failure_fingerprint", pa.string()),
    ("failure_class", pa.string()),
]

WORKFLOWS_SCHEMA = pa.schema(_WORKFLOW_FIELDS + [("observed_at", pa.string())])

RUNS_SCHEMA = pa.schema(
    _WORKFLOW_FIELDS + [("first_seen_at", pa.string()), ("last_seen_at", pa.string())]
)


def write_table_bytes(table: pa.Table) -> bytes:
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


# File-level Parquet key-value metadata carrying the identity of the cycle
# that wrote the object. Published alongside the same id in meta.json so a
# consumer can tell whether the three objects it is reading came from one
# cycle or from a publication that was torn partway through. See
# docs/notes/atomic-publication.md.
GENERATION_ID_KEY = b"generation_id"


def stamp_generation(table: pa.Table, generation_id: str) -> pa.Table:
    return table.replace_schema_metadata({GENERATION_ID_KEY: generation_id})


def table_to_parquet_bytes(rows, schema, generation_id: str | None = None) -> bytes:
    table = pa.Table.from_pylist(rows, schema=schema)
    if generation_id is not None:
        table = stamp_generation(table, generation_id)
    return write_table_bytes(table)


def read_generation_id(data: bytes) -> str | None:
    """Reads a stored object's generation id out of its file metadata, or
    returns None for an object written before generations existed (or for
    bytes that are not Parquet at all — callers decide which of those is an
    error)."""
    metadata = pq.read_schema(io.BytesIO(data)).metadata or {}
    value = metadata.get(GENERATION_ID_KEY)
    return value.decode() if value is not None else None


def conform(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """Re-shape a table read back from storage to `schema`, filling columns it
    does not have with nulls and dropping ones the schema no longer declares.

    This is what makes a schema change survivable. The run ledger is read back
    and rewritten every cycle, so without this the first cycle after a release
    that adds a column would fail to concatenate old rows with new ones — and
    because the cycle is retried on the same stale object every interval, it
    would not recover on its own. Losing a column's history is acceptable;
    a crash-loop that also stops collecting is not.
    """
    columns = []
    for field in schema:
        if field.name in table.column_names:
            columns.append(table.column(field.name).cast(field.type))
        else:
            columns.append(pa.nulls(table.num_rows, type=field.type))
    return pa.Table.from_arrays(columns, schema=schema)


def parquet_bytes_to_table(data, schema) -> pa.Table:
    """Reads stored Parquet bytes, or returns an empty table on the first run
    (nothing written yet)."""
    if data is None:
        return pa.Table.from_pylist([], schema=schema)
    return conform(pq.read_table(io.BytesIO(data)), schema)
