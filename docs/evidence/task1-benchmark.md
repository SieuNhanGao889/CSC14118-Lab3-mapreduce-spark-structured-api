# Task 1 final benchmark

Command:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\benchmark-task1.ps1 -Runs 5 -Pipeline all
```

Configuration and scope: the fixed environment in `environment-summary.md`;
each value is the job-emitted end-to-end elapsed time and includes YARN
scheduling, all MapReduce jobs in that pipeline, shuffle/reduce work, and final
single-file publication.

| Pipeline | Runs | Raw samples (s) | Mean (s) | Sample SD (s) |
|---|---:|---|---:|---:|
| Task 1-1 | 5 | 58.662, 57.715, 58.851, 58.319, 58.769 | 58.463 | 0.465 |
| Task 1-2 global (submitted) | 5 | 84.130, 77.957, 78.885, 76.822, 83.600 | 80.279 | 3.360 |
| Task 1-2 local sensitivity | 5 | 52.286, 52.370, 50.886, 51.408, 51.900 | 51.770 | 0.624 |

All three pipelines completed successfully in every measured run. The global
Task 1-2 pipeline has one additional job/raw-input pass to determine the 1,103
globally eligible styles, so its runtime is not directly interchangeable with
the two-job local-sensitivity pipeline.
