package lab3.tools

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.Instant

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Reproducible, read-only profiling for the Lab 3 Amazon sales dataset.
 * - Input: CSV file with header, UTF-8 encoding, and no quoted newlines.
 * - Xác minh cấu trúc và toàn vẹn kiểm tra 24 cột, đếm tổng số bản ghi OrderID là duy nhất và phát hiện các bản ghi trùng lặp theo cột index.
 * - kiểm tra chuẩn hóa các cột Date, ship-state, promotion-ids để phân tích thống kê
 * - Thông kế tính toán các số liệu thống kê: tổng số bản ghi, phạm vi ngày, số lượng bản ghi có ngày hợp lệ, số lượng bản ghi có giá trị Qty
 * - Output: Markdown report with metrics, distributions, and missing values.
 */
object DataProfiler {
  private val RequiredColumns = Seq(
    "index", "Order ID", "Date", "Status", "Fulfilment", "Sales Channel ",
    "ship-service-level", "Style", "SKU", "Category", "Size", "ASIN",
    "Courier Status", "Qty", "currency", "Amount", "ship-city", "ship-state",
    "ship-postal-code", "ship-country", "promotion-ids", "B2B", "fulfilled-by",
    "Unnamed: 22"
  )

  private def missing(c: Column): Column = c.isNull || length(trim(c)) === 0

  private def markdownTable(headers: Seq[String], rows: Seq[Seq[Any]]): String = {
    val header = headers.mkString(" | ")
    val separator = headers.map(_ => "---").mkString(" | ")
    val body = rows.map(_.map(v => Option(v).getOrElse("").toString.replace("|", "\\|")).mkString(" | ")).mkString("\n")
    Seq(header, separator, body).filter(_.nonEmpty).mkString("\n")
  }

  private def writeUtf8(path: String, content: String): Unit = {
    val target: Path = Paths.get(path).toAbsolutePath.normalize()
    Option(target.getParent).foreach(parent => Files.createDirectories(parent))
    Files.writeString(target, content, StandardCharsets.UTF_8)
  }

  def main(args: Array[String]): Unit = {
    val input = args.headOption.getOrElse("/workspace/input/amazon_sales.csv")
    val reportPath = args.lift(1).getOrElse("/workspace/output/data-profile.md")

    val spark = SparkSession.builder().appName("Lab3-DataProfiler").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try {
      val raw = spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        .option("encoding", "UTF-8")
        .csv(input)
        .cache()

      val missingColumns = RequiredColumns.filterNot(raw.columns.contains)
      require(missingColumns.isEmpty, s"Missing required columns: ${missingColumns.mkString(", ")}")

      val rowCount = raw.count()
      val parsedDate = to_date(trim(col("Date")), "MM-dd-yy")
      val qtyIsInteger = trim(col("Qty")).rlike("^[+-]?[0-9]+$")
      val amountIsNumeric = trim(col("Amount")).rlike("^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)$")
      val normalizedState = upper(trim(col("ship-state")))
      val promotionCount = when(missing(col("promotion-ids")), lit(0)).otherwise(
        size(filter(split(col("promotion-ids"), ","), token => length(trim(token)) > 0))
      )

      val profiled = raw
        .withColumn("__date", parsedDate)
        .withColumn("__month", date_format(parsedDate, "yyyy-MM"))
        .withColumn("__state", normalizedState)
        .withColumn("__promotion_count", promotionCount)
        .cache()

      val nullRows = RequiredColumns.map { name =>
        Seq(name, raw.filter(missing(col(name))).count())
      }

      val dateSummary = profiled.agg(
        min(col("__date")).as("min_date"),
        max(col("__date")).as("max_date"),
        count(col("__date")).as("parsed_dates")
      ).first()

      val invalidDates = rowCount - dateSummary.getAs[Long]("parsed_dates")
      val invalidQty = raw.filter(!missing(col("Qty")) && !qtyIsInteger).count()
      val invalidAmount = raw.filter(!missing(col("Amount")) && !amountIsNumeric).count()
      val normalizedStates = profiled.filter(!missing(col("ship-state"))).select("__state").distinct().count()
      val apoRows = profiled.filter(col("__state") === "APO").count()
      val distinctOrders = raw.select(col("Order ID")).filter(!missing(col("Order ID"))).distinct().count()
      val duplicateIndexRows = raw.groupBy(col("index")).count().filter(col("count") > 1).agg(coalesce(sum(col("count") - 1), lit(0L))).first().getLong(0)

      val maxSkuMonth = profiled
        .filter(col("SKU").isNotNull && col("__month").isNotNull)
        .groupBy(col("SKU"), col("__month"))
        .count()
        .orderBy(desc("count"), asc("SKU"), asc("__month"))
        .limit(1)
        .collect()
        .headOption

      val statusRows = raw.groupBy(trim(col("Status")).as("value")).count()
        .orderBy(desc("count"), asc("value")).collect().map(r => Seq(r.getString(0), r.getLong(1))).toSeq
      val sizeRows = raw.groupBy(trim(col("Size")).as("value")).count()
        .orderBy(desc("count"), asc("value")).collect().map(r => Seq(r.getString(0), r.getLong(1))).toSeq
      val stateRows = profiled.groupBy(col("__state").as("value")).count()
        .orderBy(desc("count"), asc_nulls_last("value")).collect().map(r => Seq(Option(r.getString(0)).getOrElse("<NULL>"), r.getLong(1))).toSeq

      val promotionSummary = profiled.agg(
        min(col("__promotion_count")).as("min_promotions"),
        max(col("__promotion_count")).as("max_promotions"),
        avg(col("__promotion_count")).as("avg_promotions")
      ).first()

      val maxSkuMonthText = maxSkuMonth
        .map(r => s"${r.getAs[String]("SKU")} / ${r.getAs[String]("__month")} (${r.getAs[Long]("count")} records)")
        .getOrElse("n/a")

      val report =
        s"""# Lab 3 data profile
           |
           |Generated by `lab3.tools.DataProfiler` at ${Instant.now()}.
           |
           |## Input integrity
           |
           || Metric | Value |
           || --- | ---: |
           || Input | `${input}` |
           || Rows | ${rowCount} |
           || Columns | ${raw.columns.length} |
           || Distinct non-empty Order IDs | ${distinctOrders} |
           || Duplicate index rows | ${duplicateIndexRows} |
           || Parsed dates | ${dateSummary.getAs[Long]("parsed_dates")} |
           || Invalid dates | ${invalidDates} |
           || Date range | ${dateSummary.getAs[java.sql.Date]("min_date")} to ${dateSummary.getAs[java.sql.Date]("max_date")} |
           || Invalid non-empty Qty values | ${invalidQty} |
           || Invalid non-empty Amount values | ${invalidAmount} |
           || Non-empty states after UPPER/TRIM | ${normalizedStates} |
           || APO anomaly rows | ${apoRows} |
           || Promotion count range | ${promotionSummary.getAs[Int]("min_promotions")} to ${promotionSummary.getAs[Int]("max_promotions")} |
           || Mean promotions per record | ${"%.4f".formatLocal(java.util.Locale.ROOT, promotionSummary.getAs[Double]("avg_promotions"))} |
           || Largest SKU-month group | ${maxSkuMonthText} |
           |
           |## Missing values
           |
           |${markdownTable(Seq("Column", "Null or blank rows"), nullRows)}
           |
           |## Status distribution
           |
           |${markdownTable(Seq("Normalized display value", "Rows"), statusRows)}
           |
           |## Size distribution
           |
           |${markdownTable(Seq("Size", "Rows"), sizeRows)}
           |
           |## State distribution after UPPER/TRIM
           |
           |This is a profiling view, not the final state canonicalization. See `docs/data_rules.md`.
           |
           |${markdownTable(Seq("State token", "Rows"), stateRows)}
           |""".stripMargin

      writeUtf8(reportPath, report)
      println(s"Profile written to $reportPath")
      println(s"Rows=$rowCount Columns=${raw.columns.length} DateRange=${dateSummary.getAs[java.sql.Date]("min_date")}..${dateSummary.getAs[java.sql.Date]("max_date")}")
    } finally {
      spark.stop()
    }
  }
}
