import pyarrow as pa
import pyarrow.parquet as pq
import io

from src.parquet_io import (
    RUNS_SCHEMA,
    WORKFLOWS_SCHEMA,
    conform,
    parquet_bytes_to_table,
    read_generation_id,
    table_to_parquet_bytes,
)


def test_snapshot_roundtrip_preserves_values_and_nulls():
    rows = [
        {
            "uid": "uid-1", "cluster": "ci", "namespace": "argo", "name": "build-abcde",
            "template": "build", "template_scope": "namespaced", "trigger_kind": "event",
            "trigger_name": "build", "phase": "Succeeded", "message": None, "progress": "2/2",
            "created_at": "2026-08-11T03:31:30Z", "started_at": "2026-08-11T03:31:31Z",
            "finished_at": "2026-08-11T03:32:33Z", "duration_seconds": 62,
            "resources_duration_cpu": 31, "resources_duration_memory": 605,
            "failed_step": None, "failed_step_message": None,
            "failure_fingerprint": None, "failure_class": None,
            "observed_at": "2026-08-11T04:00:00Z",
        }
    ]
    table = pq.read_table(io.BytesIO(table_to_parquet_bytes(rows, WORKFLOWS_SCHEMA)))
    assert table.to_pylist() == rows


def test_empty_input_still_writes_a_readable_table():
    data = table_to_parquet_bytes([], WORKFLOWS_SCHEMA)
    assert parquet_bytes_to_table(data, WORKFLOWS_SCHEMA).num_rows == 0


def test_missing_object_reads_as_an_empty_table_of_the_right_schema():
    table = parquet_bytes_to_table(None, RUNS_SCHEMA)
    assert table.num_rows == 0
    assert table.schema == RUNS_SCHEMA


def test_conform_backfills_a_column_added_by_a_later_release():
    """A ledger written before a column existed must still merge afterwards —
    the alternative is a cycle that fails on the same stored object forever."""
    old = pa.Table.from_pylist(
        [{"uid": "uid-1", "phase": "Succeeded", "first_seen_at": "2026-08-11T03:00:00Z",
          "last_seen_at": "2026-08-11T04:00:00Z"}],
        schema=pa.schema([("uid", pa.string()), ("phase", pa.string()),
                          ("first_seen_at", pa.string()), ("last_seen_at", pa.string())]),
    )
    conformed = conform(old, RUNS_SCHEMA)
    assert conformed.schema == RUNS_SCHEMA
    row = conformed.to_pylist()[0]
    assert row["uid"] == "uid-1"
    assert row["failed_step"] is None
    # Phase 3a's columns are additive in exactly this way: rows written by
    # 0.1.0 carry no failure taxonomy and read back as null rather than as a
    # crash on the first cycle after the upgrade.
    assert row["failure_fingerprint"] is None
    assert row["failure_class"] is None


def test_failure_columns_survive_a_ledger_roundtrip():
    """The taxonomy is written as opaque strings and reads back unchanged."""
    rows = [{
        "uid": "uid-1", "cluster": "ci", "namespace": "argo", "name": "build-abcde",
        "phase": "Failed", "message": "failed step 'test'",
        "failed_step": "test", "failed_step_message": "error[E0432]: unresolved import",
        "failure_fingerprint": "8f49197fbc86", "failure_class": "build",
        "first_seen_at": "2026-08-11T03:00:00Z", "last_seen_at": "2026-08-11T04:00:00Z",
    }]
    table = pq.read_table(io.BytesIO(table_to_parquet_bytes(rows, RUNS_SCHEMA)))
    [row] = table.to_pylist()
    for key, value in rows[0].items():
        assert row[key] == value, key


def test_conform_drops_a_column_a_later_release_removed():
    old = pa.Table.from_pylist(
        [{"uid": "uid-1", "retired_column": "x"}],
        schema=pa.schema([("uid", pa.string()), ("retired_column", pa.string())]),
    )
    assert conform(old, RUNS_SCHEMA).schema == RUNS_SCHEMA


def test_generation_id_roundtrips_through_file_metadata():
    data = table_to_parquet_bytes([], RUNS_SCHEMA, "2026-09-23T19:00:00Z-abc123")
    assert read_generation_id(data) == "2026-09-23T19:00:00Z-abc123"
    # An empty publication is still a generation: the id lives in the file
    # metadata, not in the rows, so a zero-row snapshot is pairable too.
    assert parquet_bytes_to_table(data, RUNS_SCHEMA).num_rows == 0


def test_object_written_without_a_generation_reads_back_as_none():
    assert read_generation_id(table_to_parquet_bytes([], WORKFLOWS_SCHEMA)) is None


def test_reading_a_stored_ledger_drops_its_old_generation_id():
    """conform() rebuilds the table on the current schema, so a generation id
    read back from storage cannot leak into the object the next cycle writes:
    each publication stamps its own id, never inherits the previous one."""
    stored = table_to_parquet_bytes(
        [{"uid": "uid-1", "first_seen_at": "2026-09-22T00:00:00Z",
          "last_seen_at": "2026-09-22T00:00:00Z"}],
        RUNS_SCHEMA, "2026-09-22T00:00:00Z-oldgen",
    )
    read_back = parquet_bytes_to_table(stored, RUNS_SCHEMA)
    rewritten = table_to_parquet_bytes(read_back.to_pylist(), RUNS_SCHEMA)
    assert read_generation_id(rewritten) is None
