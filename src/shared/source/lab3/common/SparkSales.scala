package lab3.common

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/** Shared Spark ingestion. It performs normalization but no task-specific filtering. 
  * đọc dữ liệu thô dạng StringType đặt chế độ FAILFAST
  * Xử lý chuỗi mã khuyến mãi
  * Chuẩn hóa vùng miền và ngày tháng 
  * tạo các cờ logic 
  * output: DataFreame với các cột đã chuẩn hóa và các cờ logic
  * Xử lý hàng ngang đồng loạt trên toàn bộ cột (Catalyst Optimizer)
  */
object SparkSales {
  val RawSchema: StructType = StructType(LabRules.RawHeaders.map(StructField(_, StringType, nullable = true)))

  def read(spark: SparkSession, path: String): DataFrame = {
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val raw = spark.read
      .schema(RawSchema)
      .option("header", "true")
      .option("enforceSchema", "false")
      .option("mode", "FAILFAST")
      .option("encoding", "UTF-8")
      .csv(path)

    val state = normalized(col("ship-state"))
    val promotionTokens = filter(
      transform(split(coalesce(col("promotion-ids"), lit("")), ","), token => trim(token)),
      token => length(token) > 0
    )
    val promotions = array_distinct(promotionTokens)
    val status = normalized(col("Status"))
    val quantity = strictInteger(col("Qty"))
    val parsedDate = to_date(trim(col("Date")), LabRules.DatePattern)
    val isShipped = coalesce(status.contains("SHIPPED"), lit(false))
    val isCancelled = coalesce(status.contains("CANCELLED"), lit(false))

    raw.select(
      strictLong(col("index")).as("source_index"),
      optional(col("Order ID")).as("order_id"),
      parsedDate.as("order_date"),
      status.as("status"),
      normalized(col("Fulfilment")).as("fulfilment"),
      normalized(col("Sales Channel ")).as("sales_channel"),
      normalized(col("ship-service-level")).as("ship_service_level"),
      optional(col("Style")).as("style"),
      optional(col("SKU")).as("sku"),
      optional(col("Category")).as("category"),
      normalized(col("Size")).as("size"),
      optional(col("ASIN")).as("asin"),
      normalized(col("Courier Status")).as("courier_status"),
      quantity.as("quantity"),
      normalized(col("currency")).as("currency"),
      strictDouble(col("Amount")).as("amount"),
      normalized(col("ship-city")).as("ship_city"),
      state.as("ship_state"),
      regexp_replace(optional(col("ship-postal-code")), "[.]0$", "").as("ship_postal_code"),
      normalized(col("ship-country")).as("ship_country"),
      promotions.as("promotion_ids"),
      size(promotions).as("promotion_count"),
      (size(promotionTokens) - size(promotions)).as("duplicate_promotion_count"),
      strictBoolean(col("B2B")).as("is_b2b"),
      normalized(col("fulfilled-by")).as("fulfilled_by"),
      date_format(parsedDate, LabRules.MonthPattern).as("order_month"),
      sizeRank(normalized(col("Size"))).as("size_rank"),
      isShipped.as("is_shipped"),
      isCancelled.as("is_cancelled"),
      (isShipped && coalesce(quantity > 0, lit(false))).as("is_bought"),
      (state.isNotNull && state =!= "APO").as("is_valid_state")
    )
  }

  private def optional(value: Column): Column =
    when(value.isNull || length(trim(value)) === 0, lit(null).cast(StringType)).otherwise(trim(value))

  private def normalized(value: Column): Column = upper(optional(value))

  private def strictLong(value: Column): Column = {
    val text = optional(value)
    when(text.rlike("^[+-]?[0-9]+$"), text.cast("long")).otherwise(lit(null).cast("long"))
  }

  private def strictInteger(value: Column): Column = {
    val text = optional(value)
    when(text.rlike("^[+-]?[0-9]+$"), text.cast("int")).otherwise(lit(null).cast("int"))
  }

  private def strictDouble(value: Column): Column = {
    val text = optional(value)
    when(text.rlike("^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)$"), text.cast("double"))
      .otherwise(lit(null).cast("double"))
  }

  private def strictBoolean(value: Column): Column = normalized(value) match {
    case text => when(text === "TRUE", lit(true)).when(text === "FALSE", lit(false)).otherwise(lit(null).cast("boolean"))
  }

  private def sizeRank(value: Column): Column =
    element_at(typedLit(LabRules.SizeRanks), value)
}
