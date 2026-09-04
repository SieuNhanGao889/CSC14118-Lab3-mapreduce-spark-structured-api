# Task 1-1 — dynamic sliding window

The implementation uses two MapReduce jobs:

1. Count bought records per normalized state to choose a 5-day or 10-day window.
2. Map each bought record into its future window-date buckets, combine moments per
   `(state, window_date, size)`, then use secondary sorting to select the winning size.

The graded baseline normalizes states with `UPPER(TRIM(value))`, matching the
reference slide and its 3,696-row acceptance check. Geographic alias merging is not
applied to the submitted result.

Build and run from the project root:

```powershell
docker compose --profile tools run --rm build mvn clean package
docker cp .\target\lab3-1.0.0-all.jar lab3-namenode:/tmp/lab3-all.jar
docker exec lab3-namenode hadoop jar /tmp/lab3-all.jar lab3.task11.Task11 `
  /lab3/input/amazon_sales.csv `
  /lab3/work/task-1-1 `
  /workspace/output/Task_1-1.csv
```

The final CSV is written to `output/Task_1-1.csv`; intermediate files remain under
`/lab3/work/task-1-1` in HDFS.
