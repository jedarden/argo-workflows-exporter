import pyarrow as pa
import pyarrow.parquet as pq
import io
import pytest

from src.parquet_io import (
    RUNS_SCHEMA,
    WORKFLOWS_SCHEMA,
    conform,
    parquet_bytes_to_table,
    read_generation_id,
    table_to_parquet_bytes,
)


# The published column contract, pinned: docs/notes/output-schema.md promises
# consumers these columns, and both files differ only in their trailing
# timestamp columns. Changing the schema should be a conscious edit here, the
# way src/meta_schema.py makes a meta.json change one.
SHARED_COLUMN_NAMES = [
    "uid", "cluster", "namespace", "name", "template", "template_scope",
    "trigger_kind", "trigger_name", "phase", "message", "progress",
    "created_at", "started_at", "finished_at", "duration_seconds",
    "resources_duration_cpu", "resources_duration_memory", "failed_step",
    "failed_step_message", "failure_fingerprint", "failure_class",
]

INTEGER_COLUMN_NAMES = [
    "duration_seconds", "resources_duration_cpu", "resources_duration_memory",
]


def test_workflows_schema_is_the_shared_columns_plus_observed_at():
    assert WORKFLOWS_SCHEMA.names == SHARED_COLUMN_NAMES + ["observed_at"]


def test_runs_schema_is_the_shared_columns_plus_ledger_timestamps():
    assert RUNS_SCHEMA.names == SHARED_COLUMN_NAMES + ["first_seen_at", "last_seen_at"]


def test_shared_columns_carry_the_same_types_in_both_schemas():
    # Both files are documented as sharing one schema; drift between the two
    # would break a consumer reading the pair with a single code path.
    for name in SHARED_COLUMN_NAMES:
        assert WORKFLOWS_SCHEMA.field(name).type == RUNS_SCHEMA.field(name).type, name


@pytest.mark.parametrize("schema", [WORKFLOWS_SCHEMA, RUNS_SCHEMA], ids=["workflows", "runs"])
def test_columns_are_strings_apart_from_the_three_int64_counters(schema):
    for name in schema.names:
        expected = pa.int64() if name in INTEGER_COLUMN_NAMES else pa.string()
        assert schema.field(name).type == expected, name


@pytest.mark.parametrize("schema", [WORKFLOWS_SCHEMA, RUNS_SCHEMA], ids=["workflows", "runs"])
def test_every_column_is_nullable(schema):
    # The exporter never enforces NOT NULL and must not: a running run has no
    # duration or finished_at yet, an inline workflow no template. Readers are
    # entitled to null in any column at any time.
    assert all(field.nullable for field in schema)


@pytest.mark.parametrize("schema", [WORKFLOWS_SCHEMA, RUNS_SCHEMA], ids=["workflows", "runs"])
def test_the_written_file_declares_the_schema_consumers_read(schema):
    """Downstream readers key on the footer's schema, so what lands in the
    object must name and type every column exactly as declared here — a
    pyarrow type mapping surprise shows up here, not in a consumer."""
    stored = pq.read_schema(io.BytesIO(table_to_parquet_bytes([], schema)))
    assert stored.names == schema.names
    for name in schema.names:
        assert stored.field(name).type == schema.field(name).type, name
        assert stored.field(name).nullable == schema.field(name).nullable, name


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


@pytest.mark.parametrize("schema", [WORKFLOWS_SCHEMA, RUNS_SCHEMA], ids=["workflows", "runs"])
def test_empty_input_still_writes_a_readable_table(schema):
    table = parquet_bytes_to_table(table_to_parquet_bytes([], schema), schema)
    assert table.num_rows == 0
    assert table.schema == schema


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


def test_ledger_roundtrip_preserves_values_types_and_nulls():
    """The full ledger schema end to end: a run in flight (nulls where a live
    run legitimately has none — duration, finished_at, the failure columns)
    beside a failed one with the int counters and the taxonomy set. Every
    column crosses Parquet once, so a type drift on any of them fails here
    rather than in a consumer."""
    running = {
        "uid": "uid-2", "cluster": "ci", "namespace": "argo", "name": "train-abcde",
        "template": "train", "template_scope": "namespaced", "trigger_kind": "cron",
        "trigger_name": "nightly-train", "phase": "Running", "message": None,
        "progress": None, "created_at": "2026-09-23T10:00:00Z",
        "started_at": "2026-09-23T10:00:01Z", "finished_at": None,
        "duration_seconds": None, "resources_duration_cpu": None,
        "resources_duration_memory": None, "failed_step": None,
        "failed_step_message": None, "failure_fingerprint": None,
        "failure_class": None,
        "first_seen_at": "2026-09-23T10:00:05Z", "last_seen_at": "2026-09-23T11:00:00Z",
    }
    failed = {
        "uid": "uid-1", "cluster": "prod", "namespace": "argo", "name": "build-abcde",
        "template": "build", "template_scope": "namespaced", "trigger_kind": "event",
        "trigger_name": "push", "phase": "Failed", "message": "failed step 'test'",
        "progress": "1/3", "created_at": "2026-09-22T09:00:00Z",
        "started_at": "2026-09-22T09:00:01Z", "finished_at": "2026-09-22T09:01:03Z",
        "duration_seconds": 62, "resources_duration_cpu": 31,
        "resources_duration_memory": 605, "failed_step": "test",
        "failed_step_message": "error[E0432]: unresolved import",
        "failure_fingerprint": "8f49197fbc86", "failure_class": "build",
        "first_seen_at": "2026-09-22T09:00:05Z", "last_seen_at": "2026-09-22T09:01:10Z",
    }
    rows = [running, failed]
    table = parquet_bytes_to_table(table_to_parquet_bytes(rows, RUNS_SCHEMA), RUNS_SCHEMA)
    # conform() rebuilt it on the declared schema: no file metadata, no
    # column the schema does not declare, none missing.
    assert table.schema == RUNS_SCHEMA
    assert table.to_pylist() == rows


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


# The ledger as a pre-0.2.0 release wrote it: the failure taxonomy is additive
# (docs/notes/output-schema.md), so a stored file from before that release
# legitimately lacks both columns.
_PRE_TAXONOMY_RUNS_SCHEMA = pa.schema(
    [f for f in RUNS_SCHEMA if f.name not in ("failure_fingerprint", "failure_class")]
)


def test_a_ledger_file_from_before_the_failure_taxonomy_reads_back_current():
    """The stored object must survive an upgrade on the first cycle that reads
    it — exercised through parquet_bytes_to_table, the path a cycle actually
    takes, not just through conform() on an in-memory table."""
    stored = table_to_parquet_bytes(
        [{
            "uid": "uid-1", "cluster": "ci", "namespace": "argo", "name": "build-abcde",
            "template": "build", "template_scope": "namespaced", "trigger_kind": "event",
            "trigger_name": "push", "phase": "Failed", "message": "failed step 'test'",
            "progress": "1/3", "created_at": "2026-08-11T03:31:30Z",
            "started_at": "2026-08-11T03:31:31Z", "finished_at": "2026-08-11T03:32:33Z",
            "duration_seconds": 62, "resources_duration_cpu": 31,
            "resources_duration_memory": 605, "failed_step": "test",
            "failed_step_message": "error[E0432]: unresolved import",
            "first_seen_at": "2026-08-11T03:31:35Z", "last_seen_at": "2026-08-11T03:32:40Z",
        }],
        _PRE_TAXONOMY_RUNS_SCHEMA,
    )
    table = parquet_bytes_to_table(stored, RUNS_SCHEMA)
    assert table.schema == RUNS_SCHEMA
    [row] = table.to_pylist()
    # everything the old file carried is still there, at its original type
    assert row["uid"] == "uid-1"
    assert row["failed_step"] == "test"
    assert row["duration_seconds"] == 62
    assert row["last_seen_at"] == "2026-08-11T03:32:40Z"
    # the columns the upgrade added arrive as nulls, not as an error
    assert row["failure_fingerprint"] is None
    assert row["failure_class"] is None


def test_a_stored_column_the_schema_dropped_is_dropped_and_its_neighbors_survive():
    """The other direction of an upgrade: a release retired a column, but the
    objects already written with it still sit in the bucket. Dropping it must
    not take the neighboring values down with it."""
    stored = table_to_parquet_bytes(
        [
            {"uid": "uid-1", "phase": "Succeeded", "retired_column": "x"},
            {"uid": "uid-2", "phase": "Failed", "retired_column": None},
        ],
        pa.schema([("uid", pa.string()), ("phase", pa.string()),
                   ("retired_column", pa.string())]),
    )
    table = parquet_bytes_to_table(stored, RUNS_SCHEMA)
    assert table.schema == RUNS_SCHEMA
    assert "retired_column" not in table.column_names
    assert table.column("uid").to_pylist() == ["uid-1", "uid-2"]
    assert table.column("phase").to_pylist() == ["Succeeded", "Failed"]


def test_an_upgrade_that_both_adds_and_removes_columns_needs_a_single_read():
    """A release can do both at once; the stored file must conform in one read
    rather than needing one migration per direction."""
    stored = table_to_parquet_bytes(
        [{"uid": "uid-1", "phase": "Succeeded", "first_seen_at": "2026-08-11T03:00:00Z",
          "last_seen_at": "2026-08-11T04:00:00Z", "retired_column": "x"}],
        pa.schema([("uid", pa.string()), ("phase", pa.string()),
                   ("first_seen_at", pa.string()), ("last_seen_at", pa.string()),
                   ("retired_column", pa.string())]),
    )
    table = parquet_bytes_to_table(stored, RUNS_SCHEMA)
    assert table.schema == RUNS_SCHEMA
    assert "retired_column" not in table.column_names
    [row] = table.to_pylist()
    assert row["uid"] == "uid-1"
    assert row["phase"] == "Succeeded"
    assert row["failure_fingerprint"] is None


def test_column_order_in_a_stored_file_is_normalized_to_the_schema():
    """Storage order is not contractual: conform() selects columns by name and
    emits the current schema's order, so a release that reorders columns still
    reads every stored file correctly."""
    stored = table_to_parquet_bytes(
        [{"phase": "Succeeded", "uid": "uid-1"}],
        pa.schema([("phase", pa.string()), ("uid", pa.string())]),
    )
    table = parquet_bytes_to_table(stored, RUNS_SCHEMA)
    assert table.schema == RUNS_SCHEMA
    assert table.column("uid").to_pylist() == ["uid-1"]
    assert table.column("phase").to_pylist() == ["Succeeded"]
