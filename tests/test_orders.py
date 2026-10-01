"""Tests for `orders`, generated from its plan and the catalog's data contracts.

Written by agentic-pipeline-copilot (no model involved).
Plan: dedup_latest into silver from orders; keys order_id.
Fixtures `spark` and `target_table` come from the runner (the sandbox harness,
or the project's conftest). Edit the plan, not this file.
"""

from __future__ import annotations

from typing import Any

import pytest

KEYS = ['order_id']


def current(spark: Any, target_table: str) -> Any:
    """The target's rows - only current ones for SCD2 history tables."""
    frame = spark.table(target_table)
    return frame.where("is_current") if "is_current" in frame.columns else frame


def test_target_is_not_empty(spark: Any, target_table: str) -> None:
    rows = current(spark, target_table).count()
    assert rows > 0, "the notebook wrote no rows"


def test_keys_are_present(spark: Any, target_table: str) -> None:
    missing = [k for k in KEYS if k not in spark.table(target_table).columns]
    assert not missing, f"key column(s) missing from the target: {missing}"


def test_keys_are_not_null(spark: Any, target_table: str) -> None:
    condition = " OR ".join(f"`{k}` IS NULL" for k in KEYS)
    nulls = current(spark, target_table).where(condition).count()
    assert nulls == 0, f"{nulls} row(s) with a null key"


def test_one_row_per_key(spark: Any, target_table: str) -> None:
    dupes = current(spark, target_table).groupBy(*KEYS).count().where("count > 1").count()
    assert dupes == 0, f"{dupes} {'/'.join(KEYS)} value(s) appear more than once"
