# Lab 3 preparation runbook

## One-command setup

Windows may block unsigned local PowerShell scripts. Use a process-scoped bypass; this does not change the machine-wide execution policy:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\setup-lab3.ps1 -SkipPull
```

Omit `-SkipPull` when images need to be downloaded or refreshed.

## Full preparation verification

After setup succeeds, run:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\verify-preparation.ps1
```

This command performs a Maven build, verifies HDFS ingestion, runs a real MapReduce job on YARN, runs a distributed Spark CSV/Parquet round trip, and regenerates the data profile.

Generated evidence:

- `docs/environment-check.txt` — service and version audit.
- `output/data-profile.md` — reproducible dataset profile.
- `output/smoke-csv/` and `output/smoke-parquet/` — temporary format round-trip evidence.
- `/lab3/smoke/mapreduce-record-count` in HDFS — MapReduce smoke result.

The smoke artefacts are preparation evidence and must not be submitted as answers to Tasks 1-1 through 2-2.

## Service endpoints

| Service | URL |
| --- | --- |
| HDFS NameNode | http://localhost:9871 |
| YARN ResourceManager | http://localhost:8089 |
| Spark Master | http://localhost:8080 |
| Spark Worker | http://localhost:8081 |

## Useful lifecycle commands

```powershell
docker compose ps
docker compose logs --tail 100 namenode datanode1 datanode2
docker compose stop
docker compose up -d
```

Do not run `docker compose down -v` unless HDFS data loss is intended; `-v` deletes the named NameNode/DataNode volumes.
