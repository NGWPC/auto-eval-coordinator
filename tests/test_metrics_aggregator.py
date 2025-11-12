"""Unit tests for MetricsAggregator deduplication functionality."""

import sys
from pathlib import Path

import pandas as pd
import pytest

# Add src directory to path so we can import metrics_aggregator
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from metrics_aggregator import MetricsAggregator


# Fixtures
@pytest.fixture
def testdata_dir():
    """Return the path to the test data directory."""
    return Path(__file__).parent.parent / "testdata" / "metrics_deduplication"


@pytest.fixture
def load_csv_fixture(testdata_dir):
    """Factory fixture to load CSV test data files."""

    def _load_csv(scenario_name, data_type):
        """
        Load a CSV fixture file.

        Args:
            scenario_name: Name of the scenario (e.g., 'scenario1')
            data_type: Either 'existing' or 'new'

        Returns:
            pd.DataFrame: Loaded DataFrame
        """
        filepath = testdata_dir / f"{scenario_name}_{data_type}.csv"
        return pd.read_csv(filepath)

    return _load_csv


@pytest.fixture
def aggregator():
    """Create a minimal MetricsAggregator instance for testing."""
    # We only need to test _merge_dataframes, so we can pass minimal args
    return MetricsAggregator(
        outputs_path="/test/path",
        stac_results={},
        data_service=None,
    )


class TestMergeDataframes:
    """
    Test suite for _merge_dataframes deduplication logic.

    Tests focus on idempotency of metrics aggregation:
    - Exact duplicates should be removed
    - Near duplicates (within floating point precision) should be removed
    - Edge cases with missing data should be handled appropriately
    """

    def test_exact_duplicates_are_removed(self, aggregator, load_csv_fixture):
        """
        Test that exact duplicate rows are properly deduplicated.
        This ensures idempotency - running aggregation multiple times produces same result.
        """
        # Load test fixtures
        existing_df = load_csv_fixture("scenario1", "existing")
        new_df = load_csv_fixture("scenario1", "new")

        # Execute merge
        result = aggregator._merge_dataframes(existing_df, new_df)

        # Assert: Duplicate should be removed
        # existing has 2 rows, new has 1 duplicate of first row
        # Result should have 2 rows (duplicate removed)
        assert len(result) == 2, "Expected 2 rows after deduplication"

        # Verify both unique items are present
        item_ids = sorted(result["stac_item_id"].tolist())
        assert item_ids == ["12030112-ble", "12030113-usgs"]

    def test_near_duplicates_removed(self, aggregator, load_csv_fixture):
        """
        Test that near-duplicate metrics (within 5 decimal rounding) are deduplicated.
        This handles floating point errors that may occur in metric calculations.
        """
        # Load test fixtures - metrics differ only beyond 5th decimal place
        existing_df = load_csv_fixture("scenario2", "existing")
        new_df = load_csv_fixture("scenario2", "new")

        # Execute merge
        result = aggregator._merge_dataframes(existing_df, new_df)

        # Assert: Should have 1 row (near-duplicate detected via rounding)
        assert len(result) == 1, "Expected 1 row after near-duplicate deduplication"
        assert result["collection_id"].iloc[0] == "ble-collection"
        assert result["scenario"].iloc[0] == "100yr"

    def test_missing_index_columns_raises_error(self, aggregator, load_csv_fixture):
        """
        Test that missing index columns (collection_id, scenario) raises an error.
        This is a data quality issue that should be caught early.
        """
        # Load test fixtures with missing collection_id or scenario
        existing_df = load_csv_fixture("scenario3", "existing")
        new_df = load_csv_fixture("scenario3", "new")

        # Execute merge - should raise ValueError
        with pytest.raises(ValueError, match="Missing required index columns|collection_id|scenario"):
            aggregator._merge_dataframes(existing_df, new_df)

    def test_no_metrics_columns_logs_warning(self, aggregator, caplog):
        """
        Test that when no numeric metrics columns exist, a warning is logged
        and data is concatenated without deduplication.
        """
        # Create dataframes with no numeric columns (only metadata)
        existing_df = pd.DataFrame(
            {
                "collection_id": ["ble-collection"],
                "stac_item_id": ["12030109-ble"],
                "scenario": ["100yr"],
                "flow": ["/path/to/flow1.csv"],
                "nws_lid": ["LID001"],
                "hucs": ["12030109"],
            }
        )

        new_df = pd.DataFrame(
            {
                "collection_id": ["ble-collection"],
                "stac_item_id": ["12030110-ble"],
                "scenario": ["500yr"],
                "flow": ["/path/to/flow2.csv"],
                "nws_lid": ["LID002"],
                "hucs": ["12030110"],
            }
        )

        # Execute merge
        with caplog.at_level("WARNING"):
            result = aggregator._merge_dataframes(existing_df, new_df)

        # Assert: Warning logged and all data concatenated
        assert "No numeric metrics columns found" in caplog.text
        assert len(result) == 2, "Should concatenate all rows when no metrics present"
