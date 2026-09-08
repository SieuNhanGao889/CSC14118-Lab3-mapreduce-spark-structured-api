# Final evidence bundle

This directory is the submission-support bundle assembled and revalidated on
2026-09-08. All reported timings use the same workstation, input file, JAR,
Docker Compose cluster, and runtime configuration documented in
`environment-summary.md`. The full plan transcript and screenshots were already
present in the repository; their metrics and result equivalence were reproduced
successfully in the final run.

## Evidence map

| Artifact | What it demonstrates |
|---|---|
| `environment-summary.md` | Fixed software, cluster, input, and benchmark configuration |
| `environment-check.txt` | Full PowerShell transcript from the final environment verification |
| `artifact-hashes.txt` | SHA-256 identities of the final input, JAR, and four deliverables |
| `data-profile.md` | Reproducible input profile and task-relevant cardinalities |
| `task1-benchmark.md` | Five end-to-end samples for Task 1-1 and both Task 1-2 interpretations |
| `task1-validation.txt` | Independent Java recomputation of both submitted MapReduce outputs |
| `task21-benchmark.md` | Five end-to-end Task 2-1 samples plus final-write stage counts |
| `task21-plan-summary.md` | Broadcast/no-broadcast plan metrics and equivalence validation |
| `task21-plan-comparison.txt` | Full extended Spark plan transcript retained for inspection |
| `task21-broadcast-joins.png` | Readable broadcast-plan screenshot |
| `task21-broadcast-disabled.png` | Readable no-broadcast-plan screenshot |
| `task21-metrics-validation.png` | Plan metric and validation screenshot |
| `task22-benchmark-validation.md` | Five samples per algorithm, accuracy differences, and validation |
| `final-output-validation.txt` | Pandas read-back plus independent Pandas/NumPy recomputation |

The text artifacts are the primary evidence because their values are searchable
and reproducible. Screenshots are supplementary and are included where the
assignment specifically asks students to inspect the Task 2-1 physical plan.

## Reproduction entry points

```powershell
powershell -ExecutionPolicy Bypass -File .\verify-preparation.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\benchmark-task1.ps1 -Runs 5 -Pipeline all
powershell -ExecutionPolicy Bypass -File .\scripts\benchmark-task21.ps1 -Runs 5
python .\scripts\validate-final-outputs.py
```

Task 2-2 is benchmarked by its Scala driver itself: one unmeasured warm-up,
followed by five interleaved measured actions for each method. See the command
in `task22-benchmark-validation.md`.
