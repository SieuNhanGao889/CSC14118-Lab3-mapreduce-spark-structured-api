package lab3.common

import java.util.UUID

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

/** Writes Spark output as the single normal-filesystem file required by the lab. 
  * kiểm tra môi trường đảm bảo đường dẫn ghi dữ liệu là local filesystem
  * tạo thư mục tạm
  * gom dữ liệu coalesce(1) về 1 partition duy nhất và ghi ra thư mục tạm
  * quét thư mục tạm để tìm file part-*.csv hoặc part-*.parquet
  * di chuyển hoặc đổi tên file part-*.csv hoặc part-*.parquet sang đường dẫn đích mong muốn rồi xóa thư mục tạm
  * Limitations: chỉ dùng được với local filesystem, không dùng được với HDFS hoặc S3. và coalesce(1) có thể gây ra vấn đề về hiệu năng nếu dữ liệu quá lớn, vì tất cả dữ liệu sẽ được gom về 1 partition duy nhất.
  */
object SingleFileOutput {
  sealed trait Format {
    private[common] def name: String
    private[common] def partSuffix: String
  }
  case object Csv extends Format {
    val name = "csv"
    val partSuffix = ".csv"
  }
  case object Parquet extends Format {
    val name = "parquet"
    val partSuffix = ".parquet"
  }

  def write(
      data: DataFrame,
      outputFile: String,
      format: Format,
      options: Map[String, String] = Map.empty
  ): Unit = {
    val configuration = data.sparkSession.sparkContext.hadoopConfiguration
    val target = new Path(outputFile)
    val fileSystem = target.getFileSystem(configuration)
    require(fileSystem.getUri.getScheme == "file", s"Final output must use the normal filesystem: $outputFile")

    val parent = Option(target.getParent).getOrElse(new Path("."))
    val temporary = new Path(parent, s".${target.getName}.tmp-${UUID.randomUUID()}")

    if (fileSystem.exists(temporary)) fileSystem.delete(temporary, true)
    try {
      data.coalesce(1).write
        .format(format.name)
        .options(options)
        .mode("overwrite")
        .save(temporary.toString)

      val partFiles = fileSystem.listStatus(temporary)
        .filter(status => status.isFile && status.getPath.getName.startsWith("part-") && status.getPath.getName.endsWith(format.partSuffix))
      require(partFiles.length == 1, s"Expected exactly one ${format.name} part file, found ${partFiles.length}")

      if (fileSystem.exists(target) && !fileSystem.delete(target, false))
        throw new IllegalStateException(s"Could not replace output file: $outputFile")
      if (!fileSystem.rename(partFiles.head.getPath, target))
        throw new IllegalStateException(s"Could not move Spark part file to: $outputFile")
    } finally {
      if (fileSystem.exists(temporary)) fileSystem.delete(temporary, true)
    }
  }
}
