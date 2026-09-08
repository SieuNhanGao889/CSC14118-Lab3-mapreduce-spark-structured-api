# Final-run environment

- Run date: 2026-09-08 (Asia/Ho_Chi_Minh)
- Host execution: Docker Desktop, Docker Engine 29.5.2
- Spark: 4.2.0, Scala 2.13.18, Java 17.0.19
- Hadoop: 3.5.0
- Build: Maven 3.9.11 with Eclipse Temurin 17
- Spark cluster: one master, one worker, one executor, two executor cores, 1 GiB executor memory
- Spark SQL shuffle partitions: 8
- Hadoop cluster: one NameNode, two live DataNodes, one ResourceManager, two running NodeManagers
- Input: `data/raw/Amazon Sale Report.csv`
- Input size: 68,923,428 bytes
- Parsed data rows: 128,975
- Parsed columns: 24
- Observed date range: 2022-03-31 through 2022-06-29
- MapReduce input: `/lab3/input/amazon_sales.csv` in HDFS
- Spark input: `/workspace/input/amazon_sales.csv` read-only bind mount
- Output directory: host-mounted `/workspace/output`

## Benchmark policy

- Every table reports raw samples, arithmetic mean, and **sample** standard deviation.
- Task 1 and Task 2-1 measurements are end-to-end driver times emitted by the jobs.
- Task 2-2 measurements intentionally compare forced analytical actions after one
  warm-up on the same cached canonical DataFrame; final Parquet publication is
  outside those per-method timings.
- No timing from another workstation or an earlier report draft is mixed into the
  final tables.
- This is a small, single-worker teaching cluster. Timings establish a reproducible
  local comparison and must not be generalized as production-scale throughput.

## Preparation checks

`verify-preparation.ps1` passed before the final benchmark sequence:

- Docker Compose services healthy;
- HDFS reports two live DataNodes;
- YARN reports two running NodeManagers;
- Maven clean package succeeds;
- MapReduce smoke count is 128,976 physical lines (header plus data);
- Spark CSV and Parquet smoke read-back is 100/100 rows;
- data profile reports 128,975 rows and 24 columns.
