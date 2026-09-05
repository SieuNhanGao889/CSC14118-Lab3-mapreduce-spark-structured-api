# Task 1-2 — state-month median variety

Primary interpretation (`local`): within each `(state, month)`, a style is eligible
only when that style serves a ranked size of at least XXL in the same group. Variety
is the number of distinct SKU of that style in the group. The final value is the
median of eligible style varieties.

The implementation uses two MapReduce jobs:

1. Secondary-sort `(state, month, style, sku)`, count distinct SKU without a HashSet,
   and retain locally eligible styles.
2. Group style varieties by `(state, month)`, sort the small list, and calculate the
   exact median (average the two middle values when the count is even).

Build and run the submitted local interpretation:

```powershell
docker compose --profile tools run --rm build mvn clean package
docker cp .\target\lab3-1.0.0-all.jar lab3-namenode:/tmp/lab3-all.jar
docker exec lab3-namenode hadoop jar /tmp/lab3-all.jar lab3.task12.Task12 `
  /lab3/input/amazon_sales.csv `
  /lab3/work/task-1-2-local `
  /workspace/output/Task_1-2.csv `
  local
```

Run the optional global-eligibility sensitivity comparison:

```powershell
docker exec lab3-namenode hadoop jar /tmp/lab3-all.jar lab3.task12.Task12 `
  /lab3/input/amazon_sales.csv `
  /lab3/work/task-1-2-global `
  /workspace/output/Task_1-2-global-sensitivity.csv `
  global
```

Global mode adds a preliminary MapReduce job that builds the globally eligible style
set and distributes it to the variety mappers. It retains only the same state-month
domain as local mode so the two outputs can be compared row by row.
