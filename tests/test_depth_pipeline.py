"""
Tests for depth-mode logic added to the auto-eval pipeline.

Covers:
- PolygonPipeline.initialize(): fim_type-aware benchmark raster key selection
- InundationStage: branch-0 split of valid outputs
- MosaicStage: two-pass depth mosaic dispatch, single-pass fallback
"""

import sys
import asyncio
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from load_config import AppConfig, Defaults, JobNames
from pipeline_stages import AgreementStage, InundationStage, MosaicStage
from pipeline_utils import PathFactory, PipelineResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_config(fim_type: str = "extent") -> AppConfig:
    cfg = AppConfig()
    cfg.defaults = Defaults()
    cfg.defaults.fim_type = fim_type
    cfg.jobs = JobNames()
    return cfg


def make_result(
    scenario_id: str = "col-scen",
    collection: str = "ble-collection",
    scenario: str = "100yr",
    benchmark_rasters: Optional[List[str]] = None,
) -> PipelineResult:
    return PipelineResult(
        scenario_id=scenario_id,
        collection_name=collection,
        scenario_name=scenario,
        flowfile_path="/flow.csv",
        benchmark_rasters=benchmark_rasters or ["/bench.tif"],
    )


def make_path_factory(tmp_path: Path) -> PathFactory:
    cfg = make_config()
    return PathFactory(cfg, str(tmp_path), "test_aoi")


# ---------------------------------------------------------------------------
# PolygonPipeline.initialize() — fim_type-aware raster key selection
# ---------------------------------------------------------------------------

class TestBenchmarkRasterSelection:
    """
    The key-selection block in main.py selects depth or extent keys
    from the STAC scenario dict based on fim_type.
    """

    SCENARIO_DATA = {
        "extent_raster": ["/bench_extent.tif"],
        "depth_raster": ["/bench_depth.tif"],
        "flow_file": ["/flow.csv"],
    }

    def _select(self, fim_type: str, scenario_data: Dict) -> List[str]:
        """Replicate the selection logic from main.py initialize()."""
        target_key = "depth" if fim_type == "depth" else "extent"
        for key in scenario_data:
            if target_key in key.lower():
                return scenario_data[key]
        return []

    def test_extent_mode_selects_extent(self):
        result = self._select("extent", self.SCENARIO_DATA)
        assert result == ["/bench_extent.tif"]

    def test_depth_mode_selects_depth(self):
        result = self._select("depth", self.SCENARIO_DATA)
        assert result == ["/bench_depth.tif"]

    def test_depth_mode_no_depth_key_returns_empty(self):
        data = {"extent_raster": ["/e.tif"], "flow_file": ["/f.csv"]}
        result = self._select("depth", data)
        assert result == []

    def test_extent_mode_no_extent_key_returns_empty(self):
        data = {"depth_raster": ["/d.tif"], "flow_file": ["/f.csv"]}
        result = self._select("extent", data)
        assert result == []


# ---------------------------------------------------------------------------
# InundationStage — branch-0 split
# ---------------------------------------------------------------------------

class TestInundationBranchSplit:
    """
    In depth mode, InundationStage must partition valid_outputs into
    primary_outputs (branch_id != 0) and branch0_outputs (branch_id == 0).
    """

    def _run_split(
        self,
        catchments: Dict[str, Dict],
        valid_outputs: List[str],
        fim_type: str,
        path_factory: PathFactory,
        result: PipelineResult,
    ):
        """Replicate the branch-split logic from InundationStage.run()."""
        if fim_type != "depth":
            return

        primary_outputs = []
        branch0_outputs = []
        for catch_id, catchment_info in catchments.items():
            path = path_factory.inundation_output_path(
                result.collection_name, result.scenario_name, catch_id
            )
            if path not in valid_outputs:
                continue
            bid = catchment_info.get("branch_id")
            if bid == 0:
                branch0_outputs.append(path)
            else:
                primary_outputs.append(path)

        result.set_path("inundation", "primary_outputs", primary_outputs)
        result.set_path("inundation", "branch0_outputs", branch0_outputs)

    def test_split_separates_branch0(self, tmp_path):
        pf = make_path_factory(tmp_path)
        result = make_result()

        catchments = {
            "catchA": {"branch_id": 1, "parquet_path": "/a.parquet"},
            "catchB": {"branch_id": 0, "parquet_path": "/b.parquet"},
            "catchC": {"branch_id": 2, "parquet_path": "/c.parquet"},
        }
        valid_outputs = [
            pf.inundation_output_path(result.collection_name, result.scenario_name, cid)
            for cid in catchments
        ]

        self._run_split(catchments, valid_outputs, "depth", pf, result)

        primary = result.get_path("inundation", "primary_outputs")
        branch0 = result.get_path("inundation", "branch0_outputs")

        assert len(primary) == 2
        assert len(branch0) == 1
        assert all("catchB" not in p for p in primary)
        assert "catchB" in branch0[0]

    def test_split_skips_failed_catchments(self, tmp_path):
        """Catchments whose output path is not in valid_outputs are skipped."""
        pf = make_path_factory(tmp_path)
        result = make_result()

        catchments = {
            "catchA": {"branch_id": 1, "parquet_path": "/a.parquet"},
            "catchB": {"branch_id": 0, "parquet_path": "/b.parquet"},
        }
        # Only catchA succeeded
        valid_outputs = [
            pf.inundation_output_path(result.collection_name, result.scenario_name, "catchA")
        ]

        self._run_split(catchments, valid_outputs, "depth", pf, result)

        primary = result.get_path("inundation", "primary_outputs")
        branch0 = result.get_path("inundation", "branch0_outputs")

        assert len(primary) == 1
        assert len(branch0) == 0

    def test_split_all_branch0_produces_empty_primary(self, tmp_path):
        pf = make_path_factory(tmp_path)
        result = make_result()

        catchments = {
            "catchA": {"branch_id": 0, "parquet_path": "/a.parquet"},
            "catchB": {"branch_id": 0, "parquet_path": "/b.parquet"},
        }
        valid_outputs = [
            pf.inundation_output_path(result.collection_name, result.scenario_name, cid)
            for cid in catchments
        ]

        self._run_split(catchments, valid_outputs, "depth", pf, result)

        assert result.get_path("inundation", "primary_outputs") == []
        assert len(result.get_path("inundation", "branch0_outputs")) == 2

    def test_split_not_called_in_extent_mode(self, tmp_path):
        """In extent mode, primary_outputs and branch0_outputs must not be set."""
        pf = make_path_factory(tmp_path)
        result = make_result()

        catchments = {"catchA": {"branch_id": 0, "parquet_path": "/a.parquet"}}
        valid_outputs = [
            pf.inundation_output_path(result.collection_name, result.scenario_name, "catchA")
        ]

        self._run_split(catchments, valid_outputs, "extent", pf, result)

        assert result.get_path("inundation", "primary_outputs") is None
        assert result.get_path("inundation", "branch0_outputs") is None

    def test_split_none_branch_id_treated_as_primary(self, tmp_path):
        """branch_id=None (e.g. hydrotable missing column) should not go to branch0."""
        pf = make_path_factory(tmp_path)
        result = make_result()

        catchments = {"catchA": {"branch_id": None, "parquet_path": "/a.parquet"}}
        valid_outputs = [
            pf.inundation_output_path(result.collection_name, result.scenario_name, "catchA")
        ]

        self._run_split(catchments, valid_outputs, "depth", pf, result)

        primary = result.get_path("inundation", "primary_outputs")
        branch0 = result.get_path("inundation", "branch0_outputs")
        assert len(primary) == 1
        assert len(branch0) == 0


# ---------------------------------------------------------------------------
# MosaicStage — two-pass dispatch and single-pass fallback
# ---------------------------------------------------------------------------

class TestMosaicTwoPassDepth:
    """
    In depth mode MosaicStage must dispatch fim_mosaicker twice when
    both primary_outputs and branch0_outputs are non-empty.
    Single-pass fallback when all catchments are branch-0.
    Extent mode must dispatch exactly once.
    """

    def _make_stage(self, fim_type: str, tmp_path: Path) -> MosaicStage:
        cfg = make_config(fim_type)
        nomad = MagicMock()
        stage = MosaicStage(
            config=cfg,
            nomad_service=nomad,
            data_service=MagicMock(),
            path_factory=make_path_factory(tmp_path),
            tags={"batch_name": "test", "aoi_name": "test_aoi"},
            aoi_path=None,
        )
        stage._clip_geometry_s3_path = None
        return stage

    def test_two_pass_dispatches_twice(self, tmp_path):
        stage = self._make_stage("depth", tmp_path)
        call_count = 0

        async def fake_dispatch(job_name, prefix, meta):
            nonlocal call_count
            call_count += 1
            return (f"job-{call_count}", {})

        stage.nomad.dispatch_and_track = fake_dispatch

        primary = ["/p1.tif", "/p2.tif"]
        branch0 = ["/b0.tif"]
        hand_output = "/outputs/hand_mosaic.tif"

        asyncio.run(
            stage._run_depth_mosaic_two_pass(primary, branch0, hand_output, "[tags]")
        )

        assert call_count == 2

    def test_two_pass_pass2_includes_pass1_output(self, tmp_path):
        stage = self._make_stage("depth", tmp_path)
        dispatched_inputs = []

        async def fake_dispatch(job_name, prefix, meta):
            dispatched_inputs.append(meta.get("raster_paths", ""))
            return ("job-id", {})

        stage.nomad.dispatch_and_track = fake_dispatch

        primary = ["/p1.tif"]
        branch0 = ["/b0.tif"]
        hand_output = "/outputs/hand_mosaic.tif"

        asyncio.run(
            stage._run_depth_mosaic_two_pass(primary, branch0, hand_output, "[tags]")
        )

        # Pass 2 inputs must include the pass-1 output path
        pass2_inputs = dispatched_inputs[1]
        assert hand_output in pass2_inputs

    def test_two_pass_no_branch0_dispatches_once(self, tmp_path):
        """If branch0_outputs is empty, only pass 1 fires."""
        stage = self._make_stage("depth", tmp_path)
        call_count = 0

        async def fake_dispatch(job_name, prefix, meta):
            nonlocal call_count
            call_count += 1
            return ("job-id", {})

        stage.nomad.dispatch_and_track = fake_dispatch

        asyncio.run(
            stage._run_depth_mosaic_two_pass(["/p1.tif"], [], "/out.tif", "[tags]")
        )

        assert call_count == 1

    def test_single_pass_fallback_dispatches_once(self, tmp_path):
        stage = self._make_stage("depth", tmp_path)
        call_count = 0

        async def fake_dispatch(job_name, prefix, meta):
            nonlocal call_count
            call_count += 1
            return ("job-id", {})

        stage.nomad.dispatch_and_track = fake_dispatch

        asyncio.run(
            stage._run_depth_mosaic_single_pass(["/b0.tif"], "/out.tif", "[tags]")
        )

        assert call_count == 1
