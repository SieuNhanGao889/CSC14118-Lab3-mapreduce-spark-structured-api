# Task 1-2 — state-month median variety

Submitted interpretation (`global`): a style is eligible when it serves a ranked
size of at least XXL anywhere in the dataset. Variety is still the number of
distinct SKU of that style inside each `(state, month)`. This mode aligns the
submitted output with the instructor reference answer and is the driver's default.

Global mode uses three MapReduce jobs:

1. Build the distinct set of globally eligible styles and distribute it through
   Hadoop Distributed Cache.
2. Secondary-sort `(state, month, style, sku)` and count distinct SKU without a
   HashSet.
3. Group varieties by `(state, month)`, sort the small list, and calculate the
   exact median.

Build and run the submitted global interpretation:

```powershell
docker compose --profile tools run --rm build mvn clean package
docker cp .\target\lab3-1.0.0-all.jar lab3-namenode:/tmp/lab3-all.jar
docker exec lab3-namenode hadoop jar /tmp/lab3-all.jar lab3.task12.Task12 `
  /lab3/input/amazon_sales.csv `
  /lab3/work/task-1-2-global `
  /workspace/output/Task_1-2.csv `
  global
```

Run the local-eligibility sensitivity comparison:

```powershell
docker exec lab3-namenode hadoop jar /tmp/lab3-all.jar lab3.task12.Task12 `
  /lab3/input/amazon_sales.csv `
  /lab3/work/task-1-2-local `
  /workspace/output/Task_1-2-local-sensitivity.csv `
  local
```

The full global result contains 144 state-month groups. Local mode contains 128.
Sensitivity statistics compare their 128 common keys; the 16 global-only groups
remain present in the submitted global output.
