# Task 2-2 — dynamic percentiles with Spark DataFrames

`lab3.task22.Task22` implements both required approaches:

- Spark `percentile_approx` with accuracy `10000`;
- a self-implemented exact NumPy/R type-7 percentile using only DataFrame
  aggregation, sorting, indexed array access, and linear interpolation.

The driver performs one warm-up followed by at least five interleaved measured
runs per approach, reports arithmetic mean and sample standard deviation,
compares thresholds/qualifying sets/final standard deviations, analyzes group
size, writes one deterministic Parquet file, and validates it after read-back.

## Build and run

```powershell
docker compose --profile tools run --rm build mvn clean package
docker cp .\target\lab3.jar lab3-spark-master:/tmp/lab3.jar
docker exec lab3-spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.executor.instances=1 `
  --conf spark.executor.cores=2 `
  --conf spark.executor.memory=1g `
  --conf spark.sql.shuffle.partitions=8 `
  --class lab3.task22.Task22 `
  /tmp/lab3.jar `
  /workspace/input/amazon_sales.csv `
  /workspace/output/Task_2-2.parquet `
  5
```

The final line must be `TASK22_VALIDATION=PASS`.

## Verified baseline

- 128,975 accepted observations and 16,486 SKU-month groups;
- largest group: 426 records; groups over 1,000: zero;
- output: 65,944 rows (`16,486 × 2 percentiles × 2 methods`);
- approximate benchmark: `1.330443 ± 0.280872` seconds;
- exact type-7 benchmark: `1.394585 ± 0.265700` seconds;
- full independent Pandas/NumPy exact comparison: 32,972 rows, zero mismatches.

| Percentile | Threshold differs | Qualifying set differs | Final SD differs |
|---|---:|---:|---:|
| P80 | 36.7160% | 6.0779% | 2.4445% |
| P90 | 43.4793% | 1.8501% | 0.7582% |

## Output schema

| Column | Type | Meaning |
|---|---|---|
| `sku` | string | SKU group key |
| `month` | string | calendar month (`yyyy-MM`) |
| `method` | string | `approx` or `exact_type7` |
| `percentile` | double | `0.8` or `0.9` |
| `threshold` | double | group-specific promotion-count threshold |
| `total_order_count` | long | all source records in the SKU-month group |
| `qualifying_order_count` | long | records at or above the threshold |
| `qualifying_amount_count` | long | qualifying records with non-null Amount |
| `amount_stddev_pop` | double | population SD; zero when fewer than two valid amounts |
