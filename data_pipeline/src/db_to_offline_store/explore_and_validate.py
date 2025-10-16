from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
from great_expectations.dataset import PandasDataset

SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from utils import logger  # noqa: E402

my_logger = logger.get_logger()

DATA_SOURCE = SRC_DIR.parent / "data_sources" / "driver_stats.parquet"


def _run_expectations(df: pd.DataFrame) -> Tuple[bool, List[str], List[str]]:
    """Run a handful of Great Expectations checks over the dataframe."""
    dataset = PandasDataset(df, expectation_suite_name="driver_stats_suite")

    expectation_results: List[Tuple[str, Dict[str, object]]] = [
        (
            "datetime_not_null",
            dataset.expect_column_values_to_not_be_null("datetime"),
        ),
        (
            "driver_id_not_null",
            dataset.expect_column_values_to_not_be_null("driver_id"),
        ),
        (
            "conv_rate_between_0_and_1",
            dataset.expect_column_values_to_be_between(
                "conv_rate", min_value=0.0, max_value=1.0
            ),
        ),
        (
            "acc_rate_between_0_and_1",
            dataset.expect_column_values_to_be_between(
                "acc_rate", min_value=0.0, max_value=1.0
            ),
        ),
        (
            "avg_daily_trips_reasonable",
            dataset.expect_column_values_to_be_between(
                "avg_daily_trips", min_value=0, max_value=1_000
            ),
        ),
    ]

    successes: List[str] = []
    failures: List[str] = []

    for name, result in expectation_results:
        success = bool(result.get("success"))
        unexpected = result.get("result", {}).get("unexpected_count", "n/a")
        total = result.get("result", {}).get("element_count", "n/a")
        if success:
            successes.append(f"{name} OK (unexpected={unexpected}, total={total})")
        else:
            failures.append(f"{name} FAILED (unexpected={unexpected}, total={total})")

    return len(failures) == 0, successes, failures


def validate_driver_stats(parquet_path: Path = DATA_SOURCE) -> None:
    if not parquet_path.exists():
        raise FileNotFoundError(f"Missing data file for validation: {parquet_path}")

    my_logger.info("Reading source dataset: %s", parquet_path)
    df = pd.read_parquet(parquet_path)
    my_logger.info("Dataset has %s rows and %s columns.", len(df), len(df.columns))

    is_successful, success_msgs, failure_msgs = _run_expectations(df)

    for msg in success_msgs:
        my_logger.info("[GE] %s", msg)

    if not is_successful:
        for msg in failure_msgs:
            my_logger.error("[GE] %s", msg)
        raise RuntimeError("Great Expectations validation failed. Check logs above.")

    my_logger.info("Great Expectations validation passed for driver_stats.parquet.")


def main() -> None:
    validate_driver_stats()


if __name__ == "__main__":
    main()
