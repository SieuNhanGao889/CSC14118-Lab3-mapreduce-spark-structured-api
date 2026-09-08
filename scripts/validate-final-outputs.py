"""Independent Pandas/NumPy validation for the four final deliverables."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


EXPECTED_SCHEMAS = {
    "Task_1-1.csv": [
        "state", "window_date", "size", "purchase_count", "population_variance"
    ],
    "Task_1-2.csv": [
        "state", "month", "median_variety", "eligible_style_count", "eligibility_scope"
    ],
    "Task_2-1.parquet": [
        "ship_city", "cancelled_standard_order_count", "qualifying_order_count",
        "qualifying_percentage"
    ],
    "Task_2-2.parquet": [
        "sku", "month", "method", "percentile", "threshold", "total_order_count",
        "qualifying_order_count", "qualifying_amount_count", "amount_stddev_pop"
    ],
}


def normalized(series: pd.Series) -> pd.Series:
    result = series.astype("string").str.strip().str.upper()
    return result.mask(result.eq(""))


def optional(series: pd.Series) -> pd.Series:
    result = series.astype("string").str.strip()
    return result.mask(result.eq(""))


def promotion_ids(value: object) -> tuple[str, ...]:
    if value is None or pd.isna(value):
        return ()
    return tuple(dict.fromkeys(token.strip() for token in str(value).split(",") if token.strip()))


def read_source(path: Path) -> pd.DataFrame:
    source = pd.read_csv(path, dtype="string", keep_default_na=True)
    source["source_index"] = pd.to_numeric(source["index"], errors="coerce")
    source["order_date"] = pd.to_datetime(source["Date"], format="%m-%d-%y", errors="coerce")
    source["month"] = source["order_date"].dt.strftime("%Y-%m")
    source["amount"] = pd.to_numeric(source["Amount"], errors="coerce")
    source["sku"] = optional(source["SKU"])
    source["ship_city"] = normalized(source["ship-city"])
    source["ship_state"] = normalized(source["ship-state"])
    source["fulfilment"] = normalized(source["Fulfilment"])
    source["courier_status"] = normalized(source["Courier Status"])
    source["ship_service_level"] = normalized(source["ship-service-level"])
    source["status"] = normalized(source["Status"])
    source["promotion_ids_parsed"] = source["promotion-ids"].map(promotion_ids)
    source["promotion_count"] = source["promotion_ids_parsed"].map(len)
    return source


def validate_files(output_dir: Path) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    for name, schema in EXPECTED_SCHEMAS.items():
        path = output_dir / name
        if not path.is_file():
            raise AssertionError(f"missing required output: {path}")
        frame = pd.read_csv(path) if path.suffix == ".csv" else pd.read_parquet(path)
        if list(frame.columns) != schema:
            raise AssertionError(f"{name}: schema mismatch: {list(frame.columns)}")
        frames[name] = frame
        print(f"OUTPUT_READBACK {name} rows={len(frame)} columns={len(frame.columns)} schema=PASS")

    expected_rows = {
        "Task_1-1.csv": 3696,
        "Task_1-2.csv": 144,
        "Task_2-1.parquet": 1434,
        "Task_2-2.parquet": 65944,
    }
    for name, count in expected_rows.items():
        if len(frames[name]) != count:
            raise AssertionError(f"{name}: expected {count} rows, found {len(frames[name])}")

    key_specs = {
        "Task_1-1.csv": ["state", "window_date"],
        "Task_1-2.csv": ["state", "month"],
        "Task_2-1.parquet": ["ship_city"],
        "Task_2-2.parquet": ["sku", "month", "method", "percentile"],
    }
    for name, keys in key_specs.items():
        duplicates = int(frames[name].duplicated(keys).sum())
        if duplicates:
            raise AssertionError(f"{name}: {duplicates} duplicate output keys")
        print(f"OUTPUT_KEYS {name} duplicate_keys=0")

    t21 = frames["Task_2-1.parquet"]
    if not t21["qualifying_percentage"].between(0.0, 100.0).all():
        raise AssertionError("Task_2-1.parquet: percentage outside [0, 100]")
    if not (t21["qualifying_order_count"] <= t21["cancelled_standard_order_count"]).all():
        raise AssertionError("Task_2-1.parquet: numerator exceeds denominator")

    t22 = frames["Task_2-2.parquet"]
    if set(t22["method"]) != {"approx", "exact_type7"}:
        raise AssertionError("Task_2-2.parquet: unexpected methods")
    if set(t22["percentile"]) != {0.8, 0.9}:
        raise AssertionError("Task_2-2.parquet: unexpected percentiles")
    return frames


def validate_task21(source: pd.DataFrame, actual: pd.DataFrame) -> None:
    appearances = [
        (promotion_id, order_date)
        for ids, order_date in zip(source["promotion_ids_parsed"], source["order_date"])
        if pd.notna(order_date)
        for promotion_id in ids
    ]
    promotion_frame = pd.DataFrame(appearances, columns=["promotion_id", "order_date"])
    periods = promotion_frame.groupby("promotion_id")["order_date"].agg(["min", "max"])
    valid_promotions = set(periods.index[(periods["max"] - periods["min"]).dt.days >= 2])

    valid_state = source["ship_state"].notna() & source["ship_state"].ne("APO")
    state_basis = source[
        valid_state
        & source["fulfilment"].eq("MERCHANT")
        & source["courier_status"].eq("SHIPPED")
        & source["amount"].notna()
    ]
    state_average = state_basis.groupby("ship_state")["amount"].mean()

    cancelled_standard = source[
        source["status"].fillna("").str.contains("CANCELLED", regex=False)
        & source["ship_service_level"].eq("STANDARD")
        & source["ship_city"].notna()
        & source["source_index"].notna()
    ].copy()
    cancelled_standard["valid_promotion_count"] = cancelled_standard["promotion_ids_parsed"].map(
        lambda ids: sum(promotion_id in valid_promotions for promotion_id in ids)
    )
    cancelled_standard["state_average"] = cancelled_standard["ship_state"].map(state_average)
    cancelled_standard["qualifies"] = (
        cancelled_standard["valid_promotion_count"].ge(3)
        & cancelled_standard["amount"].notna()
        & cancelled_standard["state_average"].notna()
        & cancelled_standard["amount"].lt(cancelled_standard["state_average"])
    )
    expected = cancelled_standard.groupby("ship_city").agg(
        cancelled_standard_order_count=("source_index", "size"),
        qualifying_order_count=("qualifies", "sum"),
    ).reset_index()
    expected["qualifying_percentage"] = (
        expected["qualifying_order_count"] * 100.0 / expected["cancelled_standard_order_count"]
    )

    merged = expected.merge(actual, on="ship_city", how="outer", suffixes=("_expected", "_actual"), indicator=True)
    count_mismatch = (
        merged["cancelled_standard_order_count_expected"].ne(merged["cancelled_standard_order_count_actual"])
        | merged["qualifying_order_count_expected"].ne(merged["qualifying_order_count_actual"])
    )
    percentage_mismatch = ~np.isclose(
        merged["qualifying_percentage_expected"],
        merged["qualifying_percentage_actual"],
        atol=1e-12,
        rtol=0.0,
        equal_nan=True,
    )
    mismatches = int((merged["_merge"].ne("both") | count_mismatch | percentage_mismatch).sum())
    print(
        "TASK21_NUMPY_VALIDATION "
        f"valid_promotions={len(valid_promotions)} expected_rows={len(expected)} mismatches={mismatches}"
    )
    if mismatches:
        raise AssertionError("Task 2-1 independent Pandas validation failed")


def validate_task22(source: pd.DataFrame, actual: pd.DataFrame) -> None:
    orders = source[source["sku"].notna() & source["month"].notna()][
        ["sku", "month", "promotion_count", "amount"]
    ]
    actual_exact = actual[actual["method"].eq("exact_type7")].copy()
    actual_exact = actual_exact.set_index(["sku", "month", "percentile"]).sort_index()

    expected_rows: list[tuple[object, ...]] = []
    largest_group = 0
    groups_over_1000 = 0
    for (sku, month), group in orders.groupby(["sku", "month"], sort=False):
        counts = group["promotion_count"].to_numpy(dtype=float)
        largest_group = max(largest_group, len(group))
        groups_over_1000 += int(len(group) > 1000)
        for percentile in (0.8, 0.9):
            threshold = float(np.quantile(counts, percentile, method="linear"))
            qualified = group[group["promotion_count"].ge(threshold)]
            amounts = qualified["amount"].dropna().to_numpy(dtype=float)
            stddev = float(np.std(amounts, ddof=0)) if len(amounts) >= 2 else 0.0
            expected_rows.append(
                (sku, month, percentile, threshold, len(group), len(qualified), len(amounts), stddev)
            )

    expected = pd.DataFrame(
        expected_rows,
        columns=[
            "sku", "month", "percentile", "threshold", "total_order_count",
            "qualifying_order_count", "qualifying_amount_count", "amount_stddev_pop"
        ],
    ).set_index(["sku", "month", "percentile"]).sort_index()

    if not expected.index.equals(actual_exact.index):
        raise AssertionError("Task 2-2 exact output keys differ from NumPy keys")
    integer_columns = ["total_order_count", "qualifying_order_count", "qualifying_amount_count"]
    integer_mismatches = int((expected[integer_columns] != actual_exact[integer_columns]).any(axis=1).sum())
    threshold_mismatches = int((~np.isclose(expected["threshold"], actual_exact["threshold"], atol=1e-12, rtol=0.0)).sum())
    stddev_mismatches = int((~np.isclose(expected["amount_stddev_pop"], actual_exact["amount_stddev_pop"], atol=1e-9, rtol=0.0)).sum())
    total_mismatches = integer_mismatches + threshold_mismatches + stddev_mismatches
    print(
        "TASK22_NUMPY_VALIDATION "
        f"exact_rows={len(expected)} largest_group={largest_group} groups_over_1000={groups_over_1000} "
        f"integer_mismatches={integer_mismatches} threshold_mismatches={threshold_mismatches} "
        f"stddev_mismatches={stddev_mismatches}"
    )
    if total_mismatches:
        raise AssertionError("Task 2-2 independent NumPy validation failed")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=Path("data/raw/Amazon Sale Report.csv"))
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    args = parser.parse_args()

    frames = validate_files(args.output_dir)
    source = read_source(args.input)
    print(f"SOURCE_READBACK rows={len(source)} columns={len(source.columns)}")
    validate_task21(source, frames["Task_2-1.parquet"])
    validate_task22(source, frames["Task_2-2.parquet"])
    print("FINAL_OUTPUT_VALIDATION=PASS")


if __name__ == "__main__":
    main()
