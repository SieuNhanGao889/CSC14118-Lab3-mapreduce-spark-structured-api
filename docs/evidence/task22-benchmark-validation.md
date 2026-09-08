# Task 2-2 final benchmark and validation

Command:

```powershell
docker exec lab3-spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.executor.instances=1 `
  --conf spark.executor.cores=2 `
  --conf spark.executor.memory=1g `
  --conf spark.sql.shuffle.partitions=8 `
  --class lab3.task22.Task22 `
  /tmp/lab3-all.jar `
  /workspace/input/amazon_sales.csv `
  /workspace/output/Task_2-2.parquet `
  5
```

`TASK22_BENCHMARK_WARMUP=PASS` was emitted before the measured actions. Both
branches use the same cached canonical order DataFrame and are interleaved.
Each timing forces aggregate checksums over every output column and excludes
the final Parquet publication.

| Run | Approximate (s) | Exact type-7 (s) |
|---:|---:|---:|
| 1 | 2.164860 | 2.415558 |
| 2 | 1.867193 | 1.587081 |
| 3 | 1.511095 | 1.384235 |
| 4 | 1.279925 | 1.519264 |
| 5 | 1.247534 | 1.301650 |
| Arithmetic mean | 1.614121 | 1.641557 |
| Sample standard deviation | 0.394983 | 0.446865 |

The exact method was about 1.70% slower by arithmetic mean in this run. The
small difference is not a general speed claim; JVM/code-generation effects are
still visible in the decreasing samples.

## Accuracy comparison

| Percentile | Groups | Threshold differs | Qualifying set differs | Final population SD differs |
|---|---:|---:|---:|---:|
| P80 | 16,486 | 6,053 (36.7160%) | 1,002 (6.0779%) | 403 (2.4445%) |
| P90 | 16,486 | 7,168 (43.4793%) | 305 (1.8501%) | 125 (0.7582%) |

Largest observed group: `JNE3405-KR-L`, 2022-04, 426 rows. No group exceeded
1,000 rows. The strongest reported final-SD difference was `J0281-SKD-XXL`,
2022-06, P90: approximate threshold 11.0 selected two rows and produced SD
701.5; exact threshold 12.2 selected one row and therefore produced SD 0.0 by
the assignment's fewer-than-two-valid-amounts rule.

Driver validation:

```text
TASK22_OUTPUT_ROWS=65944
TASK22_OUTPUT_DUPLICATE_KEYS=0
TASK22_OUTPUT_INVALID_ROWS=0
TASK22_ELAPSED_SECONDS=58.354
TASK22_VALIDATION=PASS
```

The independent script in `final-output-validation.txt` separately recomputed
all 32,972 exact group-percentile rows with NumPy type-7 quantiles and reported
zero count, threshold, or standard-deviation mismatches.
