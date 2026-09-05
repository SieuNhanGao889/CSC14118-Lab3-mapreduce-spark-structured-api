# Lab 3 — Data contract and preparation rules

This document freezes the shared interpretation of the input before implementation. It is based on the official problem statement, the instructor reference slide and reproducible profiling of `data/raw/Amazon Sale Report.csv`.

## 1. Source and reproducibility

- Authoritative input: `data/raw/Amazon Sale Report.csv`.
- The file is UTF-8 CSV with a header and quoted fields. A CSV parser must be used; splitting a physical line on commas is invalid because `promotion-ids` contains embedded commas.
- Baseline shape: 128,975 data records and 24 columns.
- Observed date range: 2022-03-31 through 2022-06-29.
- The original source is immutable. Cleaning is expressed as transformations; the raw file is never edited.
- `index` is retained only for traceability and deterministic diagnostics. It is not a business key.
- All jobs must log input path, row count, rejected/quarantined count, output path, and elapsed time.

## 2. Canonical schema

| Input column | Canonical name | Type | Rule |
| --- | --- | --- | --- |
| `index` | `source_index` | long | Required and unique in the observed file. |
| `Order ID` | `order_id` | string | `trim`; blank is invalid. |
| `Date` | `order_date` | date | Parse strictly as `MM-dd-yy` in UTC. |
| `Status` | `status` | string | `upper(trim(value))`; preserve full status text. |
| `Fulfilment` | `fulfilment` | string | `upper(trim(value))`. |
| `Sales Channel ` | `sales_channel` | string | Header contains a trailing space; canonical name removes it. |
| `ship-service-level` | `ship_service_level` | string | `upper(trim(value))`. |
| `Style` | `style` | string | `trim`; comparisons are case-sensitive after observed normalization. |
| `SKU` | `sku` | string | `trim`; blank is invalid where SKU grouping is required. |
| `Category` | `category` | string | `trim`. |
| `Size` | `size` | string | `upper(trim(value))`; map to `size_rank` below. |
| `ASIN` | `asin` | string | `trim`. |
| `Courier Status` | `courier_status` | string | `upper(trim(value))`; blank becomes null. |
| `Qty` | `quantity` | integer | Strict integer parse; do not silently coerce malformed values. |
| `currency` | `currency` | string | `upper(trim(value))`; blank becomes null. |
| `Amount` | `amount` | double/decimal | Strict numeric parse; blank becomes null. |
| `ship-city` | `ship_city` | string | `upper(trim(value))`; blank becomes null. |
| `ship-state` | `ship_state` | string | State rules in section 4. |
| `ship-postal-code` | `ship_postal_code` | string | Keep as string; remove only a terminal `.0` introduced by CSV export. |
| `ship-country` | `ship_country` | string | `upper(trim(value))`; blank becomes null. |
| `promotion-ids` | `promotion_ids` | array<string> | Parse as described in section 5. |
| `B2B` | `is_b2b` | boolean | Accept case-insensitive `true` and `false` only. |
| `fulfilled-by` | `fulfilled_by` | string | `upper(trim(value))`; blank becomes null. |
| `Unnamed: 22` | — | — | Empty export artefact; exclude from business transformations. |

Column names are resolved once at ingestion and referenced by their canonical names thereafter. Transformations must not rely on inferred numeric/date types.

## 3. Record and null policy

- The assignment and reference material operate on dataset records. One CSV row is therefore the processing unit, even when `Order ID` repeats across line items.
- Do not globally drop rows. Apply only the validity predicates needed by each aggregation.
- A null `Amount` is excluded from average, variance, and standard-deviation inputs because no monetary value can be inferred. It does not automatically remove the record from frequency/count calculations.
- A null or malformed `Date`, grouping key, `Qty`, or other task-required value is excluded from that task branch and counted in diagnostics.
- Spark aggregate functions ignore null values; this behavior must be made explicit rather than relied upon accidentally.
- If no valid amount exists for a Task 1-1 tie candidate, its variance is treated as undefined and ranked after candidates with a defined variance. Remaining ties use the required lexicographic rule.
- All variance and standard-deviation calculations use the population definition (`N`, degrees of freedom 0).

Observed null/blank counts that implementations must reproduce are recorded by `output/data-profile.md`; important baselines include 7,795 missing `Amount`, 6,872 missing `Courier Status`, 49,153 missing `promotion-ids`, and 33 missing shipping-location records.

## 4. State normalization

The graded baseline normalization is `upper(trim(ship-state))`. It collapses capitalization variants such as `Delhi`, `DELHI`, and `delhi` without making geographic substitutions. This matches the reference slide's 46-state and 3,696-row Task 1-1 checks.

The following optional alias mapping is available for sensitivity analysis, but is not applied to graded outputs because it changes the reference grouping:

| Observed token | Canonical token |
| --- | --- |
| `AR` | `ARUNACHAL PRADESH` |
| `NL` | `NAGALAND` |
| `PB` | `PUNJAB` |
| `PUNJAB/MOHALI/ZIRAKPUR` | `PUNJAB` |
| `RJ`, `RAJSTHAN`, `RAJSHTHAN` | `RAJASTHAN` |
| `ORISSA` | `ODISHA` |
| `PONDICHERRY` | `PUDUCHERRY` |
| `NEW DELHI` | `DELHI` |

Blank state values are excluded from state-level outputs. The single `APO` record is anomalous: its city is also `APO` and its postal code is inconsistent with the Indian state domain. It is quarantined from state-level output and reported separately. Other full state/territory names remain as observed after case/space normalization.

## 5. Promotions

- Parse the already CSV-decoded `promotion-ids` field by comma.
- Trim each token and discard empty tokens.
- A blank/null field becomes an empty array and has promotion count zero.
- Count every associated identifier, including identifiers whose text begins with `Amazon`, as explicitly required by the problem statement.
- Promotion identity is the complete trimmed token. Do not split on spaces or hyphens.
- Duplicate identifiers within one record, if encountered, count once for that record and must be logged. No duplicates are assumed without checking.
- A promotion's active period is `max(order_date) - min(order_date)` in whole calendar days over the entire dataset.
- A promotion is temporally valid when its active period is at least 2 days (`>= 2`), not when it appears on two dates.

## 6. Status, fulfilment, and purchase predicates

- `is_shipped`: normalized `status` contains the substring `SHIPPED`. This intentionally includes values such as `SHIPPED - DELIVERED TO BUYER`, as required by the wording “has shipped in its status” and highlighted by the reference slides.
- `is_cancelled`: normalized `status` contains `CANCELLED`.
- `is_bought`: `is_shipped AND quantity > 0`.
- Standard service: normalized `ship_service_level == "STANDARD"`.
- Merchant fulfilment: normalized `fulfilment == "MERCHANT"`.
- Shipped courier condition in Task 2-1: normalized `courier_status == "SHIPPED"`.
- Do not substitute `fulfilled-by` for `Fulfilment`; they are different source fields.

## 7. Dates, months, and windows

- Parse dates with pattern `MM-dd-yy`; for example, `04-30-22` is 2022-04-30.
- Calendar month key is `yyyy-MM` in UTC.
- In Task 1-1, the window for output date `d` excludes `d` itself:
  - five-day state: `[d-5, d-1]`;
  - ten-day state: `[d-10, d-1]`.
- Window choice is based on the total number of bought records for that state across the complete dataset: more than 10,000 means 5 days; otherwise 10 days.
- Generate output dates through `max(order_date) + state_window_length`, because later windows can still contain records from the end of the input range.
- Near the beginning/end of available data, shorter non-empty windows are valid. A window with no qualifying record produces no winning size unless the final implementation documents a required explicit empty row.

## 8. Size ordering

Business ordering is:

```text
XS=1, S=2, M=3, L=4, XL=5, XXL=6, 3XL=7, 4XL=8, 5XL=9, 6XL=10
```

`Free` is unranked and does not satisfy “at least XXL”. Lexicographic comparison is used only for the final Task 1-1 tie-break exactly as required; it must not be used to evaluate `>= XXL`.

For Task 1-2, the submitted `Task_1-2.csv` uses the full global interpretation
to align with the instructor-reference answer: a style is eligible when it
serves a ranked size of at least XXL anywhere in the dataset. The natural local
interpretation remains available as `Task_1-2-local-sensitivity.csv`. The report
must state both definitions, compare the 128 common state-month keys, and retain
the 16 additional keys that belong only to the full 144-group global result.

## 9. Percentiles and standard deviation

- Compute thresholds independently for each `(sku, month)` group.
- Approximate approach: Spark `approx_percentile`/`percentile_approx`; record the accuracy parameter used.
- Exact approach: self-implemented linear interpolation equivalent to the reference example and NumPy/R type-7 quantiles:
  - sort `n` promotion counts ascending;
  - compute `h = (n - 1) * p`;
  - interpolate between `floor(h)` and `ceil(h)`.
- Qualifying records satisfy `promotion_count >= threshold`.
- Compute population standard deviation of non-null `amount` values.
- Fewer than two qualifying, non-null amounts produces `0.0`.
- Benchmark each approach at least five measured times after one unmeasured warm-up. Report arithmetic mean and sample standard deviation of elapsed wall-clock time, with identical input, master, executor, cache, and partition settings.
- Profiling must determine whether any `(sku, month)` group exceeds 1,000 records. If none does, report that measured fact and explain why manual repartitioning is not beneficial; do not invent a qualifying group.

## 10. Output contract

- Task 1-1 and Task 1-2 each produce one physical CSV file with a header.
- Task 2-1 and Task 2-2 each produce one physical Parquet file readable by Spark and Pandas in local mode.
- Final outputs reside on a normal filesystem, not only in HDFS.
- Spark's `coalesce(1)` still writes a directory. Final packaging must move/rename the single `part-*` data file to the exact required filename and must not rename a concatenated or merged Parquet stream.
- Outputs must be deterministic: define complete sort keys before export and record schemas in the report.
- Required Drive filenames are `Task_1-1.csv`, `Task_1-2.csv`, `Task_2-1.parquet`, and `Task_2-2.parquet`.

## 11. Evidence required later in Report.pdf

- Task 1-1: bucket key/design, naive and optimized time complexity, measured/theoretical shuffle volume, combiner effect, and tie-break justification.
- Task 1-2: query framing, decomposition, implementation strategy, and the eligibility-scope assumption/sensitivity result.
- Task 2-1: `explain(true)`, physical join strategies, Exchange count, stage count, and reasoning.
- Task 2-2: approximate versus exact accuracy, at least five benchmark runs, mean and standard deviation, differing qualifying sets, and measured repartition analysis.
- Every task: input/output counts, validation invariants, output schema, execution command, and relevant Spark/Hadoop configuration.

## 12. Baseline acceptance checks

Preparation is accepted only when all of the following pass:

1. Docker Compose configuration validates and core services are healthy.
2. HDFS reports two live DataNodes; YARN reports two running NodeManagers; Spark reports one alive worker.
3. `mvn package` compiles the Scala preparation utilities.
4. The raw dataset exists at `/lab3/input/amazon_sales.csv` in HDFS.
5. MapReduce smoke output reports 128,976 physical lines (header plus 128,975 data rows).
6. Spark reads 128,975 data rows, writes and reads back CSV and Parquet smoke samples, and generates `output/data-profile.md`.
