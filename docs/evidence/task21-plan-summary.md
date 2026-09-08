# Task 2-1 physical-plan comparison

The plan comparison builds the same logical result twice: once with explicit
broadcast hints on the three small join sides and once with broadcast disabled
(`spark.sql.autoBroadcastJoinThreshold=-1`) and no hints.

| Physical-plan metric | Broadcast-hinted | Broadcast disabled |
|---|---:|---:|
| `BroadcastHashJoin` nodes | 3 | 0 |
| `SortMergeJoin` nodes | 0 | 3 |
| Hash-partitioning shuffle exchanges | 4 | 7 |
| `BroadcastExchange` nodes | 3 | 0 |
| `Sort` nodes | 0 | 6 |

Observed summary markers:

```text
TASK21_COMPARISON_INPUT_ROWS=128975
TASK21_COMPARISON_BROADCAST_ONLY_ROWS=0
TASK21_COMPARISON_NO_BROADCAST_ONLY_ROWS=0
TASK21_COMPARISON_VALIDATION=PASS
```

Bidirectional DataFrame `exceptAll` therefore found no result difference. The
pre-existing complete `explain(true)` output is retained in
`task21-plan-comparison.txt`; the three PNG files are readable supplemental
excerpts for the report. The metric table and bidirectional equivalence PASS were
reproduced with the final JAR/configuration on 2026-09-08.
