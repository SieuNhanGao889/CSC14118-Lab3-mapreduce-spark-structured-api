package lab3.task21

import lab3.common.{LabRules, SingleFileOutput, SparkSales}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * Pipeline cho Task 2-1.
 * Đơn vị xử lý là 1 dòng dữ liệu gốc (source row). `source_index` chỉ dùng để
 * gắn lại số lượng promotion hợp lệ (valid) vào đúng dòng đó sau khi join,
 * không phải dùng để group theo nghiệp vụ. Mọi phép join đều là DataFrame join
 */
object Task21Pipeline {

  final case class Frames(
      validPromotions: DataFrame,
      stateAverageAmounts: DataFrame,
      cancelledStandardAll: DataFrame,
      cancelledStandardWithCity: DataFrame,
      validPromotionCounts: DataFrame,
      result: DataFrame
  )

  def build(sales: DataFrame, useBroadcastHints: Boolean = true): Frames = {
    def smallJoinSide(data: DataFrame): DataFrame =
      if (useBroadcastHints) broadcast(data) else data

    // Một promotion được coi là valid khi: max(ngày xuất hiện) - min(ngày xuất hiện) >= 2 ngày.
    // Dùng explode (KHÔNG dùng explode_outer): nếu 1 order có mảng
    // promotion rỗng, nó không đóng góp bất kỳ "promotion identity" nào vào bảng
    // tính active period cả — nên bỏ luôn, không cần giữ lại dòng null.
    val promotionAppearances = sales
      .filter(col("order_date").isNotNull)
      .select(explode(col("promotion_ids")).as("promotion_id"), col("order_date"))

    val promotionPeriods = promotionAppearances
      .groupBy(col("promotion_id"))
      .agg(
        min(col("order_date")).as("first_order_date"),
        max(col("order_date")).as("last_order_date")
      )
      .withColumn(
        "active_days",
        datediff(col("last_order_date"), col("first_order_date")).cast("long")
      )

    val validPromotions = promotionPeriods
      .filter(col("active_days") >= lit(LabRules.MinPromotionActiveDays))
      .select("promotion_id", "first_order_date", "last_order_date", "active_days")

    // Ngưỡng trung bình theo state được tính CỐ Ý từ 2 cột Fulfilment và Courier
    // Status — chứ không phải từ 2 cột na ná tên là "fulfilled-by" hay "Status"
    // của order (dễ nhầm lẫn nên ghi chú rõ ở đây).
    val stateAverageAmounts = sales
      .filter(
        col("is_valid_state") &&
          col("fulfilment") === lit("MERCHANT") &&
          col("courier_status") === lit("SHIPPED") &&
          col("amount").isNotNull
      )
      .groupBy(col("ship_state"))
      .agg(avg(col("amount")).as("state_merchant_shipped_average_amount"))

    // Vẫn GIỮ LẠI những dòng thiếu Amount/state trong phần mẫu số (denominator).
    // Lý do: sau khi LEFT join, những dòng này đơn giản là không thể nào thỏa
    // điều kiện "Amount < state-average" (vì null), nên tự động bị loại ở bước
    // is_qualifying phía sau — không cần lọc bỏ sớm ở đây.
    val cancelledStandardAll = sales
      .filter(
        col("is_cancelled") &&
          col("ship_service_level") === lit("STANDARD")
      )
      .select(
        col("source_index"),
        col("order_id"),
        col("ship_city"),
        col("ship_state"),
        col("amount"),
        col("promotion_ids")
      )

    // Một city null/rỗng thì không phải là 1 nhóm city hợp lệ để group.
    // source_index bắt buộc phải có để join số lượng promotion hợp lệ về đúng
    // dòng, tránh nhầm lẫn giữa các dòng có cùng Order ID bị lặp lại.
    val cancelledStandardWithCity = cancelledStandardAll
      .filter(col("ship_city").isNotNull && col("source_index").isNotNull)

    // promotion_ids đã được khử trùng lặp (de-duplicate) từ bước ingest dữ liệu
    // gốc rồi, nên ở đây chỉ cần đếm số promotion khớp sau khi join với bảng
    // "valid promotion" toàn cục. Broadcast bảng valid promotions (chỉ ~185 dòng)
    // để giữ bên "candidate" (đơn hàng) làm bên đứng yên, tránh shuffle bảng lớn.
    val validPromotionCounts = cancelledStandardWithCity
      .select(
        col("source_index"),
        explode(col("promotion_ids")).as("promotion_id")
      )
      .join(
        smallJoinSide(validPromotions.select(col("promotion_id"))),
        Seq("promotion_id"),
        "inner"
      )
      .groupBy(col("source_index"))
      .agg(count(col("promotion_id")).cast("long").as("valid_promotion_count"))

    // Cả 2 bảng phụ (promotion count, state average) đều nhỏ nên broadcast được.
    // Dùng LEFT join là YÊU CẦU NGHIỆP VỤ bắt buộc: những đơn không có promotion
    // nào hoặc state không có average vẫn phải giữ lại trong mẫu số của city đó.
    val enriched = cancelledStandardWithCity.alias("orders")
      .join(
        smallJoinSide(validPromotionCounts).alias("promotion_counts"),
        Seq("source_index"),
        "left"
      )
      .join(
        smallJoinSide(stateAverageAmounts).alias("state_averages"),
        col("orders.ship_state") === col("state_averages.ship_state"),
        "left"
      )
      .select(
        col("orders.ship_city").as("ship_city"),
        col("orders.amount").as("amount"),
        // Nếu không match được (null) thì coi như 0 promotion hợp lệ
        coalesce(col("promotion_counts.valid_promotion_count"), lit(0L))
          .as("valid_promotion_count"),
        col("state_averages.state_merchant_shipped_average_amount")
          .as("state_merchant_shipped_average_amount")
      )
      .withColumn(
        "is_qualifying",
        col("valid_promotion_count") >= lit(LabRules.MinValidPromotions) &&
          col("amount").isNotNull &&
          col("state_merchant_shipped_average_amount").isNotNull &&
          col("amount") < col("state_merchant_shipped_average_amount")
      )

    // Group theo city: đếm tổng số đơn (mẫu số) và số đơn thỏa điều kiện (tử số)
    val cityCounts = enriched
      .groupBy(col("ship_city"))
      .agg(
        count(lit(1)).cast("long").as("cancelled_standard_order_count"),
        sum(when(col("is_qualifying"), lit(1L)).otherwise(lit(0L)))
          .cast("long")
          .as("qualifying_order_count")
      )

    val result = cityCounts
      .withColumn(
        "qualifying_percentage",
        col("qualifying_order_count").cast("double") * lit(100.0) /
          col("cancelled_standard_order_count").cast("double")
      )
      .select(
        col("ship_city"),
        col("cancelled_standard_order_count"),
        col("qualifying_order_count"),
        col("qualifying_percentage")
      )

    Frames(
      validPromotions,
      stateAverageAmounts,
      cancelledStandardAll,
      cancelledStandardWithCity,
      validPromotionCounts,
      result
    )
  }
}

private object Task21PlanMetrics {
  final case class Counts(
      broadcastHashJoins: Int,
      sortMergeJoins: Int,
      shuffleExchanges: Int,
      broadcastExchanges: Int,
      sortNodes: Int
  )

  def from(data: DataFrame): Counts = {
    val physicalPlan = data.queryExecution.executedPlan.toString()
    def count(token: String): Int = token.r.findAllMatchIn(physicalPlan).length

    Counts(
      broadcastHashJoins = count("BroadcastHashJoin"),
      sortMergeJoins = count("SortMergeJoin"),
      shuffleExchanges = count("Exchange hashpartitioning"),
      broadcastExchanges = count("BroadcastExchange"),
      sortNodes = count("Sort ")
    )
  }

  def print(prefix: String, counts: Counts): Unit = {
    println(s"${prefix}_BROADCAST_HASH_JOINS=${counts.broadcastHashJoins}")
    println(s"${prefix}_SORT_MERGE_JOINS=${counts.sortMergeJoins}")
    println(s"${prefix}_SHUFFLE_EXCHANGES=${counts.shuffleExchanges}")
    println(s"${prefix}_BROADCAST_EXCHANGES=${counts.broadcastExchanges}")
    println(s"${prefix}_SORT_NODES=${counts.sortNodes}")
  }
}

/** Driver Scala có thể chạy trực tiếp: explain plan, thực thi, validate rồi export Task 2-1. */
object Task21 {
  private val Usage = "Usage: Task21 <input.csv> <Task_2-1.parquet>"

  def main(args: Array[String]): Unit = {
    require(args.length == 2, Usage)

    val inputPath = args(0)
    val outputPath = args(1)
    val spark = SparkSession.builder().appName("Lab3-Task-2-1").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val startedAt = System.nanoTime()
    var sales: DataFrame = null

    try {
      println(s"TASK21_INPUT_PATH=$inputPath")
      println(s"TASK21_OUTPUT_PATH=$outputPath")

      // persist để tránh phải đọc lại file CSV nhiều lần cho các bước count() sau đó
      sales = SparkSales.read(spark, inputPath).persist(StorageLevel.MEMORY_AND_DISK)
      val inputRows = sales.count()
      val frames = Task21Pipeline.build(sales)
      val planMetrics = Task21PlanMetrics.from(frames.result)

      println("TASK21_EXTENDED_ANALYTICAL_PLAN_BEGIN")
      frames.result.explain(extended = true)  // đây là yêu cầu bắt buộc: in ra explain(true)
      println("TASK21_EXTENDED_ANALYTICAL_PLAN_END")
      Task21PlanMetrics.print("TASK21_PLAN", planMetrics)

      val validPromotionRows = frames.validPromotions.count()
      val stateThresholdRows = frames.stateAverageAmounts.count()
      val cancelledStandardRows = frames.cancelledStandardAll.count()
      val eligibleDenominatorRows = frames.cancelledStandardWithCity.count()
      val rejectedRows = cancelledStandardRows - eligibleDenominatorRows

      // coalesce(1) trước khi sort cục bộ: để có thứ tự dòng ổn định (deterministic)
      // trong file xuất ra, mà không cần thêm 1 shuffle thứ 5 chỉ để phục vụ mục
      // đích trình bày file.
      val exportData = frames.result
        .coalesce(1)
        .sortWithinPartitions(col("ship_city").asc)

      val jobGroup = s"task21-final-write-${System.nanoTime()}"
      spark.sparkContext.setJobGroup(jobGroup, "Task 2-1 final Parquet export")
      SingleFileOutput.write(exportData, outputPath, SingleFileOutput.Parquet)
      spark.sparkContext.clearJobGroup()

      // Lấy lại danh sách stage ID của riêng job "ghi file" để báo cáo số stage
      val writeStageIds = spark.sparkContext.statusTracker
        .getJobIdsForGroup(jobGroup)
        .flatMap(jobId => spark.sparkContext.statusTracker.getJobInfo(jobId))
        .flatMap(_.stageIds)
        .distinct
        .sorted

      // Đọc lại chính file vừa ghi để validate kết quả (đảm bảo export đúng)
      val written = spark.read.parquet(outputPath).persist(StorageLevel.MEMORY_AND_DISK)
      val outputRows = written.count()
      val totals: Row = written
        .agg(
          sum(col("cancelled_standard_order_count")).as("denominator"),
          sum(col("qualifying_order_count")).as("numerator"),
          min(col("qualifying_percentage")).as("min_percentage"),
          max(col("qualifying_percentage")).as("max_percentage")
        )
        .head()

      val denominator = Option(totals.getAs[java.lang.Long]("denominator")).fold(0L)(_.longValue())
      val numerator = Option(totals.getAs[java.lang.Long]("numerator")).fold(0L)(_.longValue())
      val minPercentage = Option(totals.getAs[java.lang.Double]("min_percentage")).fold(0.0)(_.doubleValue())
      val maxPercentage = Option(totals.getAs[java.lang.Double]("max_percentage")).fold(0.0)(_.doubleValue())

      // Các require() dưới đây là "sanity check" — nếu logic sai thì job crash luôn
      // thay vì âm thầm xuất ra kết quả sai.
      require(outputRows > 0L, "Task 2-1 unexpectedly produced no city groups")
      require(
        denominator == eligibleDenominatorRows,
        s"Denominator invariant failed: output=$denominator expected=$eligibleDenominatorRows"
      )
      require(numerator >= 0L && numerator <= denominator, s"Invalid numerator: $numerator / $denominator")
      require(
        minPercentage >= 0.0 && maxPercentage <= 100.0,
        s"Percentage out of range: min=$minPercentage max=$maxPercentage"
      )

      val elapsedSeconds = (System.nanoTime() - startedAt).toDouble / 1e9
      println(s"TASK21_INPUT_ROWS=$inputRows")
      println(s"TASK21_VALID_PROMOTIONS=$validPromotionRows")
      println(s"TASK21_STATE_THRESHOLDS=$stateThresholdRows")
      println(s"TASK21_CANCELLED_STANDARD_ROWS=$cancelledStandardRows")
      println(s"TASK21_REJECTED_MISSING_CITY_OR_INDEX=$rejectedRows")
      println(s"TASK21_OUTPUT_CITY_ROWS=$outputRows")
      println(s"TASK21_OUTPUT_DENOMINATOR=$denominator")
      println(s"TASK21_OUTPUT_NUMERATOR=$numerator")
      println(s"TASK21_PERCENTAGE_RANGE=$minPercentage,$maxPercentage")
      println(s"TASK21_FINAL_WRITE_STAGE_COUNT=${writeStageIds.length}")
      println(s"TASK21_FINAL_WRITE_STAGE_IDS=${writeStageIds.mkString(",")}")
      println(f"TASK21_ELAPSED_SECONDS=$elapsedSeconds%.3f")
      println("TASK21_VALIDATION=PASS")

      written.unpersist()
    } finally {
      if (sales != null) sales.unpersist()
      spark.stop()
    }
  }
}

/**
 * Thí nghiệm plan bổ sung theo slide hướng dẫn(ref): so sánh plan broadcast đang dùng với
 * cùng query khi bỏ hint và tắt auto broadcast. Hai kết quả được đối chiếu hai
 * chiều bằng EXCEPT ALL để bảo đảm thay đổi physical plan không đổi đáp án.
 */
object Task21PlanComparison {
  private val Usage = "Usage: Task21PlanComparison <input.csv>"

  def main(args: Array[String]): Unit = {
    require(args.length == 1, Usage)

    val spark = SparkSession.builder().appName("Lab3-Task-2-1-Plan-Comparison").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val originalThreshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    var sales: DataFrame = null

    try {
      sales = SparkSales.read(spark, args(0)).persist(StorageLevel.MEMORY_AND_DISK)
      val inputRows = sales.count()

      val broadcastPlan = Task21Pipeline.build(sales, useBroadcastHints = true).result
      val broadcastMetrics = Task21PlanMetrics.from(broadcastPlan)
      println("TASK21_COMPARISON_BROADCAST_EXTENDED_PLAN_BEGIN")
      broadcastPlan.explain(extended = true)
      println("TASK21_COMPARISON_BROADCAST_EXTENDED_PLAN_END")

      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1L)
      val noBroadcastPlan = Task21Pipeline.build(sales, useBroadcastHints = false).result
      val noBroadcastMetrics = Task21PlanMetrics.from(noBroadcastPlan)
      println("TASK21_COMPARISON_NO_BROADCAST_EXTENDED_PLAN_BEGIN")
      noBroadcastPlan.explain(extended = true)
      println("TASK21_COMPARISON_NO_BROADCAST_EXTENDED_PLAN_END")

      val broadcastOnlyRows = broadcastPlan.exceptAll(noBroadcastPlan).count()
      val noBroadcastOnlyRows = noBroadcastPlan.exceptAll(broadcastPlan).count()

      println(s"TASK21_COMPARISON_INPUT_ROWS=$inputRows")
      Task21PlanMetrics.print("TASK21_COMPARISON_BROADCAST", broadcastMetrics)
      Task21PlanMetrics.print("TASK21_COMPARISON_NO_BROADCAST", noBroadcastMetrics)
      println(s"TASK21_COMPARISON_BROADCAST_ONLY_ROWS=$broadcastOnlyRows")
      println(s"TASK21_COMPARISON_NO_BROADCAST_ONLY_ROWS=$noBroadcastOnlyRows")
      require(
        broadcastOnlyRows == 0L && noBroadcastOnlyRows == 0L,
        "Broadcast and no-broadcast plans produced different results"
      )
      println("TASK21_COMPARISON_VALIDATION=PASS")
    } finally {
      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", originalThreshold)
      if (sales != null) sales.unpersist()
      spark.stop()
    }
  }
}
