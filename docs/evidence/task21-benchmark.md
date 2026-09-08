# Task 2-1 final benchmark and stage evidence

Command:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\benchmark-task21.ps1 -Runs 5
```

Every run used the fixed Spark configuration in `environment-summary.md`,
executed the complete Task 2-1 driver, published the single Parquet file, read it
back, and emitted `TASK21_VALIDATION=PASS`.

| Pipeline | Runs | Raw samples (s) | Mean (s) | Sample SD (s) | Validation |
|---|---:|---|---:|---:|---|
| Task 2-1 end-to-end | 5 | 29.111, 27.124, 26.879, 27.719, 26.356 | 27.438 | 1.056 | PASS in all runs |

## Final-write stage evidence

| Run | Stage count | Stage IDs |
|---:|---:|---|
| 1 | 10 | 22, 23, 24, 25, 26, 27, 28, 29, 30, 31 |
| 2 | 10 | 22, 23, 24, 25, 26, 27, 28, 29, 30, 31 |
| 3 | 10 | 22, 23, 24, 25, 26, 27, 28, 29, 30, 31 |
| 4 | 10 | 22, 23, 24, 25, 26, 27, 28, 29, 30, 31 |
| 5 | 10 | 22, 23, 24, 25, 26, 27, 28, 29, 30, 31 |

The runtime stage count is not the same quantity as the four shuffle exchanges
in the broadcast-hinted physical plan. Broadcast construction, AQE execution,
and final publication/read-back create additional stage boundaries; numeric IDs
are run-local and are reported only to make the observation auditable.
