package lab3.task22

import lab3.common.{LabRules, SingleFileOutput, SparkSales}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/** DataFrame-only implementation of both Task 2-2 percentile approaches. */
object Task22Pipeline {
  private val GroupKeys = Seq("sku", "month")

  /** One source record is one order observation for this task. */
  def orders(sales: DataFrame): DataFrame =
    sales
      .filter(
        col("source_index").isNotNull &&
          col("sku").isNotNull &&
          col("order_month").isNotNull
      )
      .select(
        col("source_index"),
        col("sku"),
        col("order_month").as("month"),
        col("promotion_count").cast("int").as("promotion_count"),
        col("amount")
      )

  /** Spark's approximate percentile implementation, with the recorded accuracy. */
  def approximateThresholds(orders: DataFrame): DataFrame = {
    val grouped = orders
      .groupBy(GroupKeys.map(col): _*)
      .agg(
        count(lit(1)).cast("long").as("total_order_count"),
        percentile_approx(
          col("promotion_count"),
          lit(0.8),
          lit(LabRules.ApproxPercentileAccuracy)
        ).cast("double").as("p80_threshold"),
        percentile_approx(
          col("promotion_count"),
          lit(0.9),
          lit(LabRules.ApproxPercentileAccuracy)
        ).cast("double").as("p90_threshold")
      )

    grouped
      .select(
        col("sku"), col("month"),
        lit("approx").as("method"),
        lit(0.8).as("percentile"),
        col("p80_threshold").as("threshold"),
        col("total_order_count")
      )
      .unionByName(
        grouped.select(
          col("sku"), col("month"),
          lit("approx").as("method"),
          lit(0.9).as("percentile"),
          col("p90_threshold").as("threshold"),
          col("total_order_count")
        )
      )
  }

  /**
   * Exact NumPy/R type-7 percentile implemented only with DataFrame operations.
   * For sorted x and h=(n-1)p, result is x[floor(h)] plus the fractional part
   * of h times the gap to x[ceil(h)]. Spark element_at uses one-based indexes.
   */
  def exactThresholds(orders: DataFrame): DataFrame = {
    val grouped = orders
      .groupBy(GroupKeys.map(col): _*)
      .agg(
        sort_array(collect_list(col("promotion_count")), asc = true)
          .as("sorted_promotion_counts"),
        count(lit(1)).cast("long").as("total_order_count")
      )

    def interpolate(percentile: Double): DataFrame =
      grouped
        .withColumn("percentile", lit(percentile))
        .withColumn(
          "h",
          (col("total_order_count").cast("double") - lit(1.0)) * col("percentile")
        )
        .withColumn("lower_zero_index", floor(col("h")).cast("int"))
        .withColumn("upper_zero_index", ceil(col("h")).cast("int"))
        .withColumn(
          "lower_value",
          element_at(col("sorted_promotion_counts"), col("lower_zero_index") + lit(1))
            .cast("double")
        )
        .withColumn(
          "upper_value",
          element_at(col("sorted_promotion_counts"), col("upper_zero_index") + lit(1))
            .cast("double")
        )
        .withColumn(
          "threshold",
          col("lower_value") +
            (col("h") - col("lower_zero_index").cast("double")) *
              (col("upper_value") - col("lower_value"))
        )
        .select(
          col("sku"), col("month"),
          lit("exact_type7").as("method"),
          col("percentile"), col("threshold"), col("total_order_count")
        )

    interpolate(0.8).unionByName(interpolate(0.9))
  }

  /** Attach each group threshold to its rows, filter, and calculate population SD. */
  def statistics(orders: DataFrame, thresholds: DataFrame): DataFrame = {
    val qualified = orders.alias("orders")
      .join(thresholds.alias("thresholds"), GroupKeys, "inner")
      .filter(
        col("orders.promotion_count").cast("double") >= col("thresholds.threshold")
      )

    qualified
      .groupBy(
        col("sku"), col("month"), col("method"), col("percentile"),
        col("threshold"), col("total_order_count")
      )
      .agg(
        count(lit(1)).cast("long").as("qualifying_order_count"),
        count(col("orders.amount")).cast("long").as("qualifying_amount_count"),
        stddev_pop(col("orders.amount")).as("raw_amount_stddev_pop")
      )
      .withColumn(
        "amount_stddev_pop",
        when(col("qualifying_amount_count") < lit(2L), lit(0.0))
          .otherwise(coalesce(col("raw_amount_stddev_pop"), lit(0.0)))
      )
      .select(
        col("sku"), col("month"), col("method"), col("percentile"),
        col("threshold"), col("total_order_count"),
        col("qualifying_order_count"), col("qualifying_amount_count"),
        col("amount_stddev_pop")
      )
  }

  def approximateResult(orders: DataFrame): DataFrame =
    statistics(orders, approximateThresholds(orders))

  def exactResult(orders: DataFrame): DataFrame =
    statistics(orders, exactThresholds(orders))

  /** Group-level comparison including threshold, membership, and final-SD differences. */
  def comparison(
      orders: DataFrame,
      approximate: DataFrame,
      exact: DataFrame
  ): DataFrame = {
    val approximateValues = approximate.select(
      col("sku"), col("month"), col("percentile"),
      col("threshold").as("approx_threshold"),
      col("qualifying_order_count").as("approx_qualifying_order_count"),
      col("amount_stddev_pop").as("approx_amount_stddev_pop")
    )
    val exactValues = exact.select(
      col("sku"), col("month"), col("percentile"),
      col("threshold").as("exact_threshold"),
      col("qualifying_order_count").as("exact_qualifying_order_count"),
      col("amount_stddev_pop").as("exact_amount_stddev_pop")
    )

    val paired = approximateValues
      .join(exactValues, Seq("sku", "month", "percentile"), "inner")

    val membership = orders.alias("orders")
      .join(paired.alias("paired"), Seq("sku", "month"), "inner")
      .withColumn(
        "qualifies_approx",
        col("orders.promotion_count").cast("double") >= col("paired.approx_threshold")
      )
      .withColumn(
        "qualifies_exact",
        col("orders.promotion_count").cast("double") >= col("paired.exact_threshold")
      )
      .groupBy(col("sku"), col("month"), col("percentile"))
      .agg(
        sum(
          when(col("qualifies_approx") =!= col("qualifies_exact"), lit(1L))
            .otherwise(lit(0L))
        ).cast("long").as("membership_difference_count")
      )

    paired
      .join(membership, Seq("sku", "month", "percentile"), "inner")
      .withColumn(
        "threshold_differs",
        abs(col("approx_threshold") - col("exact_threshold")) > lit(1e-12)
      )
      .withColumn(
        "qualifying_set_differs",
        col("membership_difference_count") > lit(0L)
      )
      .withColumn(
        "stddev_differs",
        abs(col("approx_amount_stddev_pop") - col("exact_amount_stddev_pop")) > lit(1e-9)
      )
  }
}

/** Runnable benchmark, analysis, validation, and single-Parquet export driver. */
object Task22 {
  private val Usage = "Usage: Task22 <input.csv> <Task_2-2.parquet> [measured-runs]"

  private final case class TimingSummary(values: Vector[Double]) {
    val mean: Double = values.sum / values.length
    val sampleStddev: Double = {
      if (values.length < 2) 0.0
      else {
        val squared = values.map(value => math.pow(value - mean, 2.0)).sum
        math.sqrt(squared / (values.length - 1))
      }
    }
  }

  private def forceAllColumns(data: DataFrame): Row =
    data
      .agg(
        count(lit(1)).as("rows"),
        sum(col("threshold")).as("threshold_checksum"),
        sum(col("qualifying_order_count")).as("order_checksum"),
        sum(col("qualifying_amount_count")).as("amount_count_checksum"),
        sum(col("amount_stddev_pop")).as("stddev_checksum")
      )
      .head()

  private def measuredSeconds(build: () => DataFrame): Double = {
    val started = System.nanoTime()
    forceAllColumns(build())
    (System.nanoTime() - started).toDouble / 1e9
  }

  def main(args: Array[String]): Unit = {
    require(args.length == 2 || args.length == 3, Usage)
    val inputPath = args(0)
    val outputPath = args(1)
    val measuredRuns = if (args.length == 3) args(2).toInt else 5
    require(measuredRuns >= 5, "Task 2-2 requires at least five measured benchmark runs")

    val spark = SparkSession.builder().appName("Lab3-Task-2-2").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val startedAt = System.nanoTime()
    var sales: DataFrame = null
    var orders: DataFrame = null

    try {
      println(s"TASK22_INPUT_PATH=$inputPath")
      println(s"TASK22_OUTPUT_PATH=$outputPath")
      println(s"TASK22_APPROX_ACCURACY=${LabRules.ApproxPercentileAccuracy}")
      println(s"TASK22_MEASURED_RUNS=$measuredRuns")

      sales = SparkSales.read(spark, inputPath).persist(StorageLevel.MEMORY_AND_DISK)
      val inputRows = sales.count()
      orders = Task22Pipeline.orders(sales).persist(StorageLevel.MEMORY_AND_DISK)
      val orderRows = orders.count()

      val groupSizes = orders
        .groupBy(col("sku"), col("month"))
        .agg(count(lit(1)).cast("long").as("order_count"))
        .persist(StorageLevel.MEMORY_AND_DISK)
      val groupCount = groupSizes.count()
      val largest = groupSizes.orderBy(col("order_count").desc, col("sku"), col("month")).head()
      val largestGroupRows = largest.getAs[Long]("order_count")
      val groupsOver1000 = groupSizes.filter(col("order_count") > lit(1000L)).count()

      val inputHadoopPath = new Path(inputPath)
      val inputFileSystem = inputHadoopPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
      val inputBytes = inputFileSystem.getFileStatus(inputHadoopPath).getLen
      val estimatedLargestGroupBytes =
        math.round(inputBytes.toDouble * largestGroupRows.toDouble / inputRows.toDouble)
      val maxPartitionBytes = spark.sparkContext.getConf
        .getSizeAsBytes("spark.sql.files.maxPartitionBytes", "128m")

      val approximateBuilder = () => Task22Pipeline.approximateResult(orders)
      val exactBuilder = () => Task22Pipeline.exactResult(orders)

      // One unmeasured warm-up for each approach, followed by interleaved runs.
      forceAllColumns(approximateBuilder())
      forceAllColumns(exactBuilder())
      println("TASK22_BENCHMARK_WARMUP=PASS")

      val approximateTimings = ArrayBuffer.empty[Double]
      val exactTimings = ArrayBuffer.empty[Double]
      (1 to measuredRuns).foreach { run =>
        val approximateSeconds = measuredSeconds(approximateBuilder)
        approximateTimings += approximateSeconds
        println(f"TASK22_APPROX_RUN_${run}_SECONDS=$approximateSeconds%.6f")

        val exactSeconds = measuredSeconds(exactBuilder)
        exactTimings += exactSeconds
        println(f"TASK22_EXACT_RUN_${run}_SECONDS=$exactSeconds%.6f")
      }

      val approximateSummary = TimingSummary(approximateTimings.toVector)
      val exactSummary = TimingSummary(exactTimings.toVector)

      val approximate = approximateBuilder().persist(StorageLevel.MEMORY_AND_DISK)
      val exact = exactBuilder().persist(StorageLevel.MEMORY_AND_DISK)
      val approximateRows = approximate.count()
      val exactRows = exact.count()

      val comparison = Task22Pipeline.comparison(orders, approximate, exact)
        .persist(StorageLevel.MEMORY_AND_DISK)
      val comparisonByPercentile = comparison
        .groupBy(col("percentile"))
        .agg(
          count(lit(1)).cast("long").as("group_count"),
          sum(when(col("threshold_differs"), lit(1L)).otherwise(lit(0L)))
            .cast("long").as("threshold_difference_groups"),
          sum(when(col("qualifying_set_differs"), lit(1L)).otherwise(lit(0L)))
            .cast("long").as("qualifying_set_difference_groups"),
          sum(when(col("stddev_differs"), lit(1L)).otherwise(lit(0L)))
            .cast("long").as("stddev_difference_groups")
        )
        .orderBy(col("percentile"))
        .collect()

      val strongestDifference = comparison
        .withColumn(
          "absolute_stddev_difference",
          abs(col("approx_amount_stddev_pop") - col("exact_amount_stddev_pop"))
        )
        .orderBy(
          col("absolute_stddev_difference").desc,
          col("sku"), col("month"), col("percentile")
        )
        .head()

      val combined = approximate.unionByName(exact)
      println("TASK22_EXTENDED_PLAN_BEGIN")
      combined.explain(extended = true)
      println("TASK22_EXTENDED_PLAN_END")

      val exportData = combined
        .coalesce(1)
        .sortWithinPartitions(
          col("sku").asc,
          col("month").asc,
          col("method").asc,
          col("percentile").asc
        )
      SingleFileOutput.write(exportData, outputPath, SingleFileOutput.Parquet)

      val written = spark.read.parquet(outputPath).persist(StorageLevel.MEMORY_AND_DISK)
      val outputRows = written.count()
      val duplicateKeys = written
        .groupBy(col("sku"), col("month"), col("method"), col("percentile"))
        .count()
        .filter(col("count") =!= lit(1L))
        .count()
      val invalidRows = written.filter(
        col("sku").isNull || col("month").isNull || col("method").isNull ||
          col("threshold").isNull || col("amount_stddev_pop").isNull ||
          col("total_order_count") < lit(1L) ||
          col("qualifying_order_count") < col("qualifying_amount_count") ||
          col("qualifying_order_count") > col("total_order_count") ||
          col("amount_stddev_pop") < lit(0.0) ||
          (col("qualifying_amount_count") < lit(2L) && col("amount_stddev_pop") =!= lit(0.0))
      ).count()

      require(approximateRows == groupCount * 2L, "Approximate result lost SKU-month-percentile groups")
      require(exactRows == groupCount * 2L, "Exact result lost SKU-month-percentile groups")
      require(outputRows == groupCount * 4L, "Combined output row count invariant failed")
      require(duplicateKeys == 0L, s"Output contains $duplicateKeys duplicate keys")
      require(invalidRows == 0L, s"Output contains $invalidRows invalid rows")

      println(s"TASK22_INPUT_ROWS=$inputRows")
      println(s"TASK22_ELIGIBLE_ORDER_ROWS=$orderRows")
      println(s"TASK22_REJECTED_ROWS=${inputRows - orderRows}")
      println(s"TASK22_SKU_MONTH_GROUPS=$groupCount")
      println(s"TASK22_LARGEST_GROUP_SKU=${largest.getAs[String]("sku")}")
      println(s"TASK22_LARGEST_GROUP_MONTH=${largest.getAs[String]("month")}")
      println(s"TASK22_LARGEST_GROUP_ROWS=$largestGroupRows")
      println(s"TASK22_GROUPS_OVER_1000=$groupsOver1000")
      println(s"TASK22_INPUT_BYTES=$inputBytes")
      println(s"TASK22_ESTIMATED_LARGEST_GROUP_BYTES=$estimatedLargestGroupBytes")
      println(s"TASK22_MAX_PARTITION_BYTES=$maxPartitionBytes")
      println(s"TASK22_APPROX_TIMINGS_SECONDS=${approximateTimings.map(v => f"$v%.6f").mkString(",")}")
      println(f"TASK22_APPROX_MEAN_SECONDS=${approximateSummary.mean}%.6f")
      println(f"TASK22_APPROX_SAMPLE_STDDEV_SECONDS=${approximateSummary.sampleStddev}%.6f")
      println(s"TASK22_EXACT_TIMINGS_SECONDS=${exactTimings.map(v => f"$v%.6f").mkString(",")}")
      println(f"TASK22_EXACT_MEAN_SECONDS=${exactSummary.mean}%.6f")
      println(f"TASK22_EXACT_SAMPLE_STDDEV_SECONDS=${exactSummary.sampleStddev}%.6f")

      comparisonByPercentile.foreach { row =>
        val percentileLabel = if (math.abs(row.getAs[Double]("percentile") - 0.8) < 1e-12) "P80" else "P90"
        val groups = row.getAs[Long]("group_count")
        val thresholdDifferences = row.getAs[Long]("threshold_difference_groups")
        val setDifferences = row.getAs[Long]("qualifying_set_difference_groups")
        val stddevDifferences = row.getAs[Long]("stddev_difference_groups")
        println(s"TASK22_${percentileLabel}_GROUPS=$groups")
        println(s"TASK22_${percentileLabel}_THRESHOLD_DIFFERENCE_GROUPS=$thresholdDifferences")
        println(f"TASK22_${percentileLabel}_THRESHOLD_DIFFERENCE_PERCENT=${thresholdDifferences * 100.0 / groups}%.4f")
        println(s"TASK22_${percentileLabel}_QUALIFYING_SET_DIFFERENCE_GROUPS=$setDifferences")
        println(f"TASK22_${percentileLabel}_QUALIFYING_SET_DIFFERENCE_PERCENT=${setDifferences * 100.0 / groups}%.4f")
        println(s"TASK22_${percentileLabel}_STDDEV_DIFFERENCE_GROUPS=$stddevDifferences")
        println(f"TASK22_${percentileLabel}_STDDEV_DIFFERENCE_PERCENT=${stddevDifferences * 100.0 / groups}%.4f")
      }

      println(s"TASK22_MAX_DIFFERENCE_SKU=${strongestDifference.getAs[String]("sku")}")
      println(s"TASK22_MAX_DIFFERENCE_MONTH=${strongestDifference.getAs[String]("month")}")
      println(s"TASK22_MAX_DIFFERENCE_PERCENTILE=${strongestDifference.getAs[Double]("percentile")}")
      println(s"TASK22_MAX_DIFFERENCE_APPROX_THRESHOLD=${strongestDifference.getAs[Double]("approx_threshold")}")
      println(s"TASK22_MAX_DIFFERENCE_EXACT_THRESHOLD=${strongestDifference.getAs[Double]("exact_threshold")}")
      println(s"TASK22_MAX_DIFFERENCE_APPROX_QUALIFYING=${strongestDifference.getAs[Long]("approx_qualifying_order_count")}")
      println(s"TASK22_MAX_DIFFERENCE_EXACT_QUALIFYING=${strongestDifference.getAs[Long]("exact_qualifying_order_count")}")
      println(s"TASK22_MAX_DIFFERENCE_APPROX_STDDEV=${strongestDifference.getAs[Double]("approx_amount_stddev_pop")}")
      println(s"TASK22_MAX_DIFFERENCE_EXACT_STDDEV=${strongestDifference.getAs[Double]("exact_amount_stddev_pop")}")
      println(s"TASK22_OUTPUT_ROWS=$outputRows")
      println(s"TASK22_OUTPUT_DUPLICATE_KEYS=$duplicateKeys")
      println(s"TASK22_OUTPUT_INVALID_ROWS=$invalidRows")
      println(f"TASK22_ELAPSED_SECONDS=${(System.nanoTime() - startedAt).toDouble / 1e9}%.3f")
      println("TASK22_VALIDATION=PASS")

      written.unpersist()
      comparison.unpersist()
      approximate.unpersist()
      exact.unpersist()
      groupSizes.unpersist()
    } finally {
      if (orders != null) orders.unpersist()
      if (sales != null) sales.unpersist()
      spark.stop()
    }
  }
}
