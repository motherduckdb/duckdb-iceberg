from __future__ import annotations

import json
import shutil
import sys
import uuid
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestWriterV2,
    write_manifest_list,
)
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import LongType, NestedField

# Builds an Iceberg table whose current schema NO LONGER contains the column an equality-delete file is keyed on:
#
#   schema 0 (a:1, b:2, c:3) -> append 4 rows -> add an equality delete on field id 2 (b = 200)
#   -> drop column b, so the current schema is (a:1, c:3) while the data file and the delete file still carry b.
#
# Reading must still apply the delete (row b==200 is removed) by matching the delete's equality id (2) against the
# value of b inside the pre-drop data file. The GuaranteeEqualityDeleteColumnsOptimizer handles this by synthesising
# the dropped column; this fixture exists to check the non-optimizer fallback path (hybrid / optimizer disabled), where
# field id 2 is absent from the scan's columns. All file paths are written absolute, so read with allow_moved_paths.

DELETE_VALUE = 200  # value of column b for the row that must be deleted (row a=2)


class DeleteManifestWriterV2(ManifestWriterV2):
    """pyiceberg 0.10 has no public delete-manifest writer; flip content=DELETES."""

    def content(self) -> ManifestContent:
        return ManifestContent.DELETES

    @property
    def _meta(self) -> dict[str, str]:
        return {**super()._meta, "content": "deletes"}


def build(out_dir: Path) -> Path:
    work = out_dir / "_build"
    if work.exists():
        shutil.rmtree(work)
    warehouse = work / "warehouse"
    warehouse.mkdir(parents=True)
    catalog = SqlCatalog("r", uri=f"sqlite:///{work}/c.db", warehouse=f"file://{warehouse}")
    catalog.create_namespace("ns")
    schema = Schema(
        NestedField(1, "a", LongType(), required=False),
        NestedField(2, "b", LongType(), required=False),
        NestedField(3, "c", LongType(), required=False),
    )
    tbl = catalog.create_table("ns.t", schema=schema, properties={"format-version": "2"})
    tbl.append(
        pa.Table.from_pylist(
            [{"a": i, "b": i * 100, "c": i * 1000} for i in range(1, 5)],
            schema=pa.schema(
                [
                    pa.field("a", pa.int64(), nullable=True),
                    pa.field("b", pa.int64(), nullable=True),
                    pa.field("c", pa.int64(), nullable=True),
                ]
            ),
        )
    )
    tbl.refresh()
    base_snap = tbl.current_snapshot()
    assert base_snap is not None
    (data_manifest,) = base_snap.manifests(io=tbl.io)
    table_root = Path(tbl.location().removeprefix("file://"))

    # Equality-delete parquet keyed on field id 2 (column b). PARQUET:field_id MUST match the schema field id.
    delete_path = table_root / "data" / f"delete-{uuid.uuid4()}.parquet"
    delete_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(
            [{"b": DELETE_VALUE}],
            schema=pa.schema(
                [pa.field("b", pa.int64(), nullable=True, metadata={b"PARQUET:field_id": b"2"})]
            ),
        ),
        delete_path,
        compression="zstd",
    )

    delete_file = DataFile.from_args(
        content=DataFileContent.EQUALITY_DELETES,
        file_path=f"file://{delete_path}",
        file_format="PARQUET",
        partition=Record(),
        record_count=1,
        file_size_in_bytes=delete_path.stat().st_size,
        equality_ids=[2],
        spec_id=0,
        sort_order_id=None,
    )

    new_snap_id = 9876543210123456789
    seq = base_snap.sequence_number + 1
    delete_manifest_path = table_root / "metadata" / f"{uuid.uuid4()}-m0.avro"
    with DeleteManifestWriterV2(
        spec=PartitionSpec(spec_id=0),
        schema=tbl.schema(),
        output_file=tbl.io.new_output(f"file://{delete_manifest_path}"),
        snapshot_id=new_snap_id,
        avro_compression="gzip",
    ) as w:
        w.add(
            ManifestEntry.from_args(
                status=ManifestEntryStatus.ADDED,
                snapshot_id=new_snap_id,
                sequence_number=seq,
                file_sequence_number=seq,
                data_file=delete_file,
            )
        )
    delete_manifest = w.to_manifest_file()

    mlist_path = table_root / "metadata" / f"snap-{new_snap_id}-1-{uuid.uuid4()}.avro"
    with write_manifest_list(
        format_version=2,
        output_file=tbl.io.new_output(f"file://{mlist_path}"),
        snapshot_id=new_snap_id,
        parent_snapshot_id=base_snap.snapshot_id,
        sequence_number=seq,
        avro_compression="gzip",
    ) as mlw:
        mlw.add_manifests([data_manifest, delete_manifest])

    old_meta = json.loads(Path(tbl.metadata_location.removeprefix("file://")).read_text())

    # Add a second schema that drops column b (field id 2) and make it the current schema. The data file and delete
    # file still reference field id 2, but the table's current projection no longer exposes it.
    schema0 = old_meta["schemas"][0]
    schema1 = {
        "type": "struct",
        "schema-id": 1,
        "fields": [f for f in schema0["fields"] if f["id"] != 2],
    }
    ts = base_snap.timestamp_ms + 1000
    new_meta = {
        **old_meta,
        "schemas": [schema0, schema1],
        "current-schema-id": 1,
        "schema": schema1,
        "snapshots": list(old_meta.get("snapshots", []))
        + [
            {
                "sequence-number": seq,
                "snapshot-id": new_snap_id,
                "parent-snapshot-id": base_snap.snapshot_id,
                "timestamp-ms": ts,
                "summary": {"operation": "overwrite", "added-delete-files": "1"},
                "manifest-list": f"file://{mlist_path}",
                "schema-id": 1,
            }
        ],
        "current-snapshot-id": new_snap_id,
        "last-sequence-number": seq,
        "last-updated-ms": ts,
        "refs": {**old_meta.get("refs", {}), "main": {"snapshot-id": new_snap_id, "type": "branch"}},
    }
    (table_root / "metadata" / "v2.metadata.json").write_text(json.dumps(new_meta, indent=2))
    (table_root / "metadata" / "version-hint.text").write_text("2")

    # Publish the table directory to the destination and drop the build scratch (catalog db, etc.).
    dest = out_dir / "mytable"
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(table_root, dest)
    shutil.rmtree(work)
    return dest


# Regenerate the committed fixture with:
#   python scripts/persistent/create_equality_delete_dropped_column_table.py \
#       data/persistent/equality_delete_dropped_column
# All paths are written absolute, so the fixture must be read with allow_moved_paths=true.
if __name__ == "__main__":
    default_out = Path(__file__).resolve().parents[2] / "data" / "persistent" / "equality_delete_dropped_column"
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else default_out
    out.mkdir(parents=True, exist_ok=True)
    print(build(out))
