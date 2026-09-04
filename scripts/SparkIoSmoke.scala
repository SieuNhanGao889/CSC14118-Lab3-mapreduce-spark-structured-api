package lab3.tools

import lab3.common.SparkSales
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws}

/** Proves distributed CSV input and local-filesystem CSV/Parquet output. 
  * Nhận vào 3 tham số: input.csv, csv-output-dir, parquet-output-dir
  * đọc dữ liệu thô từ input.csv bằng SparkSales.read() -> DataFrame
  * lấy 100 dòng đầu tiên và cache() để kiểm tra round-trip I/O
  * ghi ra csv-output-dir với coalesce(1) và header=true
  * ghi ra parquet-output-dir với coalesce(1)
  * đọc lại csv-output-dir và parquet-output-dir để kiểm tra số lượng dòng có khớp với số lượng dòng ban đầu
  * nếu khớp thì in ra thông báo
  */
object SparkIoSmoke {
  def main(args: Array[String]): Unit = {
    require(args.length == 3, "Usage: SparkIoSmoke <input.csv> <csv-output-dir> <parquet-output-dir>")
    val spark = SparkSession.builder().appName("Lab3-Spark-IO-Smoke").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try {
      val sample = SparkSales.read(spark, args(0)).limit(100).cache()
      val expected = sample.count()
      sample
        .withColumn("promotion_ids", concat_ws(",", col("promotion_ids")))
        .coalesce(1)
        .write.mode("overwrite").option("header", "true").csv(args(1))
      sample.coalesce(1).write.mode("overwrite").parquet(args(2))
      val csvRows = spark.read.option("header", "true").csv(args(1)).count()
      val parquetRows = spark.read.parquet(args(2)).count()
      require(csvRows == expected, s"CSV round-trip mismatch: expected=$expected actual=$csvRows")
      require(parquetRows == expected, s"Parquet round-trip mismatch: expected=$expected actual=$parquetRows")
      println(s"Spark I/O smoke passed: input=$expected csv=$csvRows parquet=$parquetRows")
    } finally {
      spark.stop()
    }
  }
}
