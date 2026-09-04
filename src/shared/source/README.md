# Shared source

Package `lab3.common` contains only cross-task infrastructure:

- `LabRules`: constants, normalization, parsing, and shared predicates.
- `AmazonCsv`: quote-aware parsing for Hadoop MapReduce mappers.
- `SparkSales`: explicit-schema ingestion and canonical Spark columns.
- `SingleFileOutput`: safe single-file CSV/Parquet export to the local filesystem.

Task algorithms stay in their respective `Task_*/source` directories.
