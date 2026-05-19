"""
Integration tests: HandIndexQuerier surfaces branch_id from hydrotable data.

Uses real test parquet data from testdata/hand/parquet-index/ with an augmented
hydrotables parquet that includes branch_id (branch-0 and non-zero entries).
"""
import shutil
import sys
import uuid
from pathlib import Path
from typing import Dict

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from hand_index_querier import HandIndexQuerier

TESTDATA = Path(__file__).parent.parent / "testdata"
PARQUET_INDEX = TESTDATA / "hand" / "parquet-index"
QUERY_POLYGON = TESTDATA / "query-polygon.gpkg"

SECOND_UUID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")


def _duplicate_uuid_table(src_path: Path, dest_path: Path, second_uuid: uuid.UUID) -> None:
    """
    Read a parquet table with an arrow.uuid catchment_id column, duplicate the
    single row, and replace the second row's catchment_id with second_uuid.

    Preserves the arrow.uuid extension type so DuckDB reads UUIDs correctly.
    Without this, pandas writes raw binary and DuckDB returns bytearray (unhashable),
    crashing HandIndexQuerier._filter_dataframes_by_overlap.
    """
    tbl = pq.read_table(src_path)
    uuid_field_idx = tbl.schema.get_field_index("catchment_id")
    uuid_type = tbl.schema.field("catchment_id").type

    # Read the existing UUID from storage rather than hardcoding bytes.
    orig_uuid_bytes = bytes(tbl.column("catchment_id").cast(uuid_type.storage_type)[0].as_py())

    new_ids = pa.array(
        [orig_uuid_bytes, second_uuid.bytes], type=uuid_type.storage_type
    ).cast(uuid_type)

    row_dict = {col: tbl.column(col).to_pylist() * 2 for col in tbl.column_names if col != "catchment_id"}
    no_id_schema = tbl.schema.remove(uuid_field_idx)
    no_id_tbl = pa.table(row_dict, schema=no_id_schema)
    final = no_id_tbl.append_column(pa.field("catchment_id", uuid_type), new_ids)
    pq.write_table(final, dest_path)


def _build_augmented_index(tmp_path: Path) -> Path:
    """
    Copy the parquet index to tmp_path with two catchments (branch-0 and non-zero)
    and hydrotables augmented with branch_id.
    """
    dest = tmp_path / "parquet-index"
    shutil.copytree(str(PARQUET_INDEX), str(dest))

    cat_path = dest / "catchments" / "h3_index=581707621791170559" / "data_0.parquet"
    _duplicate_uuid_table(cat_path, cat_path, SECOND_UUID)

    for raster_file in ("hand_rem_rasters.parquet", "hand_catchment_rasters.parquet"):
        rp = dest / raster_file
        _duplicate_uuid_table(rp, rp, SECOND_UUID)

    # Rebuild hydrotables with branch_id column (UUID extension type preserved).
    ht_src = PARQUET_INDEX / "hydrotables.parquet"
    ht_tbl = pq.read_table(ht_src)
    uuid_type = ht_tbl.schema.field("catchment_id").type
    csv_path_val = ht_tbl.column("csv_path").to_pylist()[0]

    orig_id_bytes = bytes(ht_tbl.column("catchment_id").cast(uuid_type.storage_type)[0].as_py())
    new_ids = pa.array(
        [orig_id_bytes, SECOND_UUID.bytes], type=uuid_type.storage_type
    ).cast(uuid_type)

    ht_new = pa.table(
        {
            "catchment_id": new_ids,
            "csv_path": pa.array([csv_path_val, csv_path_val]),
            "branch_id": pa.array([0, 6781000013], type=pa.int64()),
        }
    )
    pq.write_table(ht_new, dest / "hydrotables.parquet")

    return dest


@pytest.fixture(scope="module")
def augmented_index(tmp_path_factory: pytest.TempPathFactory) -> Path:
    tmp = tmp_path_factory.mktemp("hand_index")
    return _build_augmented_index(tmp)


@pytest.fixture(scope="module")
def catchments(augmented_index: Path) -> Dict:
    polygon_gdf = gpd.read_file(str(QUERY_POLYGON))
    with HandIndexQuerier(str(augmented_index)) as querier:
        return querier.query_catchments_for_polygon(polygon_gdf)


class TestHandIndexQuerierBranchId:
    """branch_id must be surfaced in every returned catchment entry."""

    def test_catchments_not_empty(self, catchments: Dict) -> None:
        assert len(catchments) > 0, "Expected at least one catchment for the test AOI"

    def test_every_catchment_has_branch_id_key(self, catchments: Dict) -> None:
        for catch_id, info in catchments.items():
            assert "branch_id" in info, f"catchment '{catch_id}' missing branch_id key"

    def test_at_least_one_branch_id_zero(self, catchments: Dict) -> None:
        branch_ids = [info["branch_id"] for info in catchments.values()]
        assert 0 in branch_ids, f"Expected branch_id == 0 in results; got {branch_ids}"

    def test_at_least_one_nonzero_branch_id(self, catchments: Dict) -> None:
        branch_ids = [info["branch_id"] for info in catchments.values() if info["branch_id"] is not None]
        non_zero = [b for b in branch_ids if b != 0]
        assert len(non_zero) > 0, f"Expected at least one non-zero branch_id; got {branch_ids}"

    def test_branch_id_is_int_or_none(self, catchments: Dict) -> None:
        for catch_id, info in catchments.items():
            bid = info["branch_id"]
            assert bid is None or isinstance(bid, int), (
                f"catchment '{catch_id}' branch_id has unexpected type {type(bid)}: {bid}"
            )
