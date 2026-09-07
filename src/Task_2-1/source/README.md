# Task 2-1 — Spark DataFrame implementation

`lab3.task21.Task21` implements the complete query using only Spark's
DataFrame API. It derives globally valid promotions, calculates state-level
merchant/courier-shipped Amount averages, retains every cancelled Standard row
in the city denominator through LEFT joins, explains the physical plan, writes
one Parquet file, reads it back, and checks result invariants.

The processing unit is one source record, matching the shared data contract.
Repeated `Order ID` values are not collapsed. A blank city is excluded because
it is not a valid city-level grouping key; the driver logs that rejection count.

## Build and run

From the repository root:

```powershell
docker compose --profile tools run --rm build mvn clean package
docker cp .\target\lab3.jar lab3-spark-master:/tmp/lab3.jar
docker exec lab3-spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.executor.instances=1 `
  --conf spark.executor.cores=2 `
  --conf spark.executor.memory=1g `
  --conf spark.sql.shuffle.partitions=8 `
  --class lab3.task21.Task21 `
  /tmp/lab3.jar `
  /workspace/input/amazon_sales.csv `
  /workspace/output/Task_2-1.parquet
```

The submitted artifact is the single normal-filesystem file
`output/Task_2-1.parquet`, not a Spark output directory. The driver prints the
extended plan between `TASK21_EXTENDED_ANALYTICAL_PLAN_BEGIN/END` markers and
finishes with `TASK21_VALIDATION=PASS` only after reading the file back.

## Bonus plan comparison from the instructor slide

Run the same analytical query once with the production broadcast hints and
once with all broadcast selection disabled:

```powershell
docker exec lab3-spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.executor.instances=1 `
  --conf spark.executor.cores=2 `
  --conf spark.executor.memory=1g `
  --conf spark.sql.shuffle.partitions=8 `
  --class lab3.task21.Task21PlanComparison `
  /tmp/lab3.jar `
  /workspace/input/amazon_sales.csv `
  2>&1 | Tee-Object -FilePath .\docs\Task_2-1-plan-comparison.txt
```

The comparison calls `explain(true)` for both plans and checks complete result
equivalence with `EXCEPT ALL` in both directions. Expected summary:

| Metric | Broadcast | Broadcast disabled |
|---|---:|---:|
| `BroadcastHashJoin` | 3 | 0 |
| `SortMergeJoin` | 0 | 3 |
| Shuffle `Exchange` | 4 | 7 |
| `BroadcastExchange` | 3 | 0 |
| `Sort` | 0 | 6 |

The final line must be `TASK21_COMPARISON_VALIDATION=PASS`.

For report screenshots, open `docs/Task_2-1-plan-comparison.txt` in VS Code:

1. Find `TASK21_COMPARISON_BROADCAST_EXTENDED_PLAN_BEGIN`, then the following
   `== Physical Plan ==`; capture the portion showing `BroadcastHashJoin` and
   `BroadcastExchange`.
2. Find `TASK21_COMPARISON_NO_BROADCAST_EXTENDED_PLAN_BEGIN`, then its
   `== Physical Plan ==`; capture the portion showing `SortMergeJoin`, `Sort`,
   and the extra hash-partition exchanges.
3. Capture the final `TASK21_COMPARISON_*` summary lines and the `PASS` line.

On Windows, use `Win+Shift+S` and select only the readable terminal/editor
region. Add captions such as “Default/broadcast physical plan”, “Physical plan
with auto broadcast disabled”, and “Plan metrics and result-equivalence check”.

## Output schema

| Column | Spark type | Meaning |
|---|---|---|
| `ship_city` | string | `UPPER(TRIM(ship-city))` |
| `cancelled_standard_order_count` | long | denominator for the city |
| `qualifying_order_count` | long | rows satisfying both extra predicates |
| `qualifying_percentage` | double | `100 * qualifying / denominator` |
