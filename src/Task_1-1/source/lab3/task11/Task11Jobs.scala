package lab3.task11

import java.io.File
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.time.LocalDate
import java.util.Locale

import lab3.common.{AmazonCsv, LabRules}
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.{Tool, ToolRunner}

import scala.io.Source

/** Các tên dùng chung giữa driver và các task chạy phân tán. 1 object singleton private 
  * - CounterGroup: tên nhóm cho các hadoop counters để xem log và report gom vào 1 group 
  * - CacheAlias: tên file tạm dùng để cache kết quả Job 0 cho Job 1 (ví dụ: state -> totalBoughtCount)
  * - CsvHeader: header của file CSV đầu ra của Job 1:
  *   - state: tên state
  *   - window_date: ngày cửa sổ (d) mà record được phát vào bucket [d-L, d-1]
  *   - size: size xuất hiện nhiều nhất trong cửa sổ
  *   - purchase_count: số lượng record có size đó trong cửa sổ
  *   - population_variance: phương sai của lượng mua trong cửa sổ
  */
private object Task11Constants {
  val CounterGroup = "Task11"
  val CacheAlias = "task11-state-counts.tsv"
  val CsvHeader = "state,window_date,size,purchase_count,population_variance"
}

/**
 * Mapper của Job 0: đếm tổng số bought records theo state trên toàn dataset.
 * Bought = Status chứa SHIPPED và Qty > 0 (quy tắc nằm trong SaleRecord.isBought).
 * Kết quả này quyết định state dùng cửa sổ 5 ngày hay 10 ngày ở Job 1.
 */
final class StateBoughtCountMapper extends Mapper[LongWritable, Text, Text, LongWritable] {
  private val outputState = new Text()
  private val one = new LongWritable(1L)

  override def map(
      offset: LongWritable,
      value: Text,
      context: Mapper[LongWritable, Text, Text, LongWritable]#Context
  ): Unit = {
    val line = value.toString
    // TextInputFormat đưa header vào như một record bình thường, nên phải bỏ nó. 
    if (offset.get() == 0L && AmazonCsv.isHeader(line)) return
    // Tăng counter INPUT_RECORDS cho mỗi record đọc được, không tính header.
    context.getCounter(Task11Constants.CounterGroup, "INPUT_RECORDS").increment(1L)
    // Kết quả trả về kiểu Either[String, SaleRecord], Left là lỗi parse, Right là record hợp lệ.
    AmazonCsv.parse(line) match {
      case Left(_) =>
        context.getCounter(Task11Constants.CounterGroup, "MALFORMED_CSV").increment(1L)
      case Right(record) if record.isBought =>
        // match để xử lý từng case nếu record isBought, kiểm tra state hợp lệ, nếu hợp lệ thì phát ra cặp (state, 1) để đếm tổng bought records theo state.
        record.shipState.filter(_ => record.hasValidState) match {
          case Some(state) =>
            outputState.set(state)
            context.write(outputState, one)
            context.getCounter(Task11Constants.CounterGroup, "BOUGHT_RECORDS").increment(1L)
          case None =>
            context.getCounter(Task11Constants.CounterGroup, "INVALID_STATE").increment(1L)
        }
      // Nếu record không phải isBought thì bỏ qua, không phát ra gì cả.
      case Right(_) => ()
    }
  }
}

/**
 * Phép cộng Long có tính kết hợp và giao hoán, nên cùng class này dùng được cho
 * cả Combiner (cộng tại mapper) lẫn Reducer (cộng kết quả cuối của Job 0).
 */
final class LongSumReducer extends Reducer[Text, LongWritable, Text, LongWritable] {
  private val output = new LongWritable()
  // Shuffle và sort
  override def reduce(
      key: Text,
      values: java.lang.Iterable[LongWritable],
      context: Reducer[Text, LongWritable, Text, LongWritable]#Context
  ): Unit = {
    // Duyệt qua values bằng iterator()
    val iterator = values.iterator()
    var sum = 0L
    while (iterator.hasNext) sum += iterator.next().get()
    output.set(sum)
    context.write(key, output)
  }
}

/** Job1: 
 * Mapper chính của sliding window.
 *
 * Ta đảo câu hỏi từ "ngày d cần đọc những record nào?" thành "record ngày t sẽ
 * đóng góp cho những ngày nào?". Record ngày t được phát vào các bucket
 * t+1 .. t+L, tương đương record đó nằm trong [d-L, d-1] của từng ngày d.
 * input là các record đã được lọc và chuẩn hóa từ Job 0
 * output là các cặp windowkey và moments - mô tả số lượng record, tổng amount, tổng bình phương amount cho từng size trong mỗi cửa sổ.
 */
final class WindowBucketMapper extends Mapper[LongWritable, Text, WindowSizeKey, WindowMoments] {
  private val outputKey = new WindowSizeKey()
  private val outputValue = new WindowMoments()
  private var stateCounts = Map.empty[String, Long]
  // Biến var cấp class được nạp dữ liệu 1 lần trong setup() và dùng lại xuyên suốt trong map()
  override def setup(context: Mapper[LongWritable, Text, WindowSizeKey, WindowMoments]#Context): Unit = {
    // File chỉ có 46 state được Job 0 tạo và Hadoop Distributed Cache chép tới
    // từng mapper. Nhờ vậy mapper chọn L mà không phải gọi HDFS cho mỗi record.
    val cacheFile = new File(Task11Constants.CacheAlias)
    if (!cacheFile.isFile) throw new IllegalStateException(s"Missing distributed-cache file: ${cacheFile.getAbsolutePath}")

    val source = Source.fromFile(cacheFile, StandardCharsets.UTF_8.name())
    // đọc từng dòng, 
    try {
      stateCounts = source.getLines().filter(_.nonEmpty).map { line =>
        val fields = line.split("\\t", -1)
        if (fields.length != 2) throw new IllegalArgumentException(s"Malformed state-count row: $line")
        fields(0) -> fields(1).toLong
      }.toMap
    } finally source.close()
  }

  override def map(
      offset: LongWritable,
      value: Text,
      context: Mapper[LongWritable, Text, WindowSizeKey, WindowMoments]#Context
  ): Unit = {
    val line = value.toString
    if (offset.get() == 0L && AmazonCsv.isHeader(line)) return

    context.getCounter(Task11Constants.CounterGroup, "WINDOW_INPUT_RECORDS").increment(1L)
    AmazonCsv.parse(line) match {
      case Left(_) =>
        context.getCounter(Task11Constants.CounterGroup, "WINDOW_MALFORMED_CSV").increment(1L)
      case Right(record) if record.isBought =>
        (record.shipState.filter(_ => record.hasValidState), record.orderDate, record.size) match {
          case (Some(state), Some(orderDate), Some(size)) =>
            val totalBought = stateCounts.getOrElse(
              state,
              throw new IllegalStateException(s"No bought-order count was cached for state: $state")
            )
            val windowLength = LabRules.windowDays(totalBought)

            // Amount null không làm mất record khỏi frequency. Nó chỉ bị loại khỏi
            // amountCount/sum/squareSum, đúng null policy của đề.
            val (amountCount, amountSum, amountSquareSum) = record.amount match {
              case Some(amount) => (1L, amount, amount * amount)
              case None =>
                context.getCounter(Task11Constants.CounterGroup, "NULL_AMOUNT").increment(1L)
                (0L, 0.0, 0.0)
            }

            outputValue.set(size, 1L, amountCount, amountSum, amountSquareSum)
            // Bắt đầu từ 1 để cửa sổ của ngày d không bao gồm chính ngày d.
            var dayOffset = 1
            while (dayOffset <= windowLength) {
              outputKey.set(state, orderDate.plusDays(dayOffset.toLong).toEpochDay, size)
              context.write(outputKey, outputValue)
              context.getCounter(Task11Constants.CounterGroup, "BUCKETS_EMITTED").increment(1L)
              dayOffset += 1
            }
          case _ =>
            context.getCounter(Task11Constants.CounterGroup, "INVALID_TASK_FIELDS").increment(1L)
        }
      case Right(_) => ()
    }
  }
}

/**
 * Gộp sớm các tuple thống kê có cùng (state, ngày, size) ngay trên mapper.
 * Trên dữ liệu hiện tại, bước này giảm 925.395 records xuống khoảng 27 nghìn
 * records phải đi qua mạng shuffle.
 */
final class WindowMomentsCombiner
    extends Reducer[WindowSizeKey, WindowMoments, WindowSizeKey, WindowMoments] {
  private val combined = new WindowMoments()

  override def reduce(
      key: WindowSizeKey,
      values: java.lang.Iterable[WindowMoments],
      context: Reducer[WindowSizeKey, WindowMoments, WindowSizeKey, WindowMoments]#Context
  ): Unit = {
    val iterator = values.iterator()
    var purchaseCount = 0L
    var amountCount = 0L
    var amountSum = 0.0
    var amountSquareSum = 0.0
    while (iterator.hasNext) {
      val value = iterator.next()
      purchaseCount += value.purchaseCount
      amountCount += value.amountCount
      amountSum += value.amountSum
      amountSquareSum += value.amountSquareSum
    }
    combined.set(key.size, purchaseCount, amountCount, amountSum, amountSquareSum)
    context.write(key, combined)
  }
}

/**
 * Một lần reduce tương ứng một (state, window_date). Values đã được secondary
 * sort theo size, nên ta cộng từng cụm size liên tiếp rồi so với winner hiện tại.
 */
final class WindowWinnerReducer
    extends Reducer[WindowSizeKey, WindowMoments, NullWritable, Text] {
  private val output = new Text()

  override def setup(context: Reducer[WindowSizeKey, WindowMoments, NullWritable, Text]#Context): Unit = {
    // Chỉ dùng một reducer nên setup ghi đúng một header cho file CSV cuối.
    output.set(Task11Constants.CsvHeader)
    context.write(NullWritable.get(), output)
  }

  override def reduce(
      key: WindowSizeKey,
      values: java.lang.Iterable[WindowMoments],
      context: Reducer[WindowSizeKey, WindowMoments, NullWritable, Text]#Context
  ): Unit = {
    var currentSize: String = null
    var purchaseCount = 0L
    var amountCount = 0L
    var amountSum = 0.0
    var amountSquareSum = 0.0

    var bestSize: String = null
    var bestPurchaseCount = -1L
    var bestVariance: Option[Double] = None

    // Kết thúc một cụm size: tính population variance và áp dụng đủ 3 mức ưu tiên:
    // frequency lớn hơn -> variance nhỏ hơn -> size nhỏ hơn theo từ điển.
    def finishSize(): Unit = {
      if (currentSize != null) {
        val variance = if (amountCount == 0L) None else {
          val mean = amountSum / amountCount
          // Sai số floating-point đôi khi tạo số âm rất nhỏ như -1e-12.
          Some(math.max(0.0, amountSquareSum / amountCount - mean * mean))
        }
        val wins = purchaseCount > bestPurchaseCount ||
          (purchaseCount == bestPurchaseCount && compareVariance(variance, bestVariance) < 0) ||
          (purchaseCount == bestPurchaseCount && compareVariance(variance, bestVariance) == 0 &&
            (bestSize == null || currentSize.compareTo(bestSize) < 0))
        if (wins) {
          bestSize = currentSize
          bestPurchaseCount = purchaseCount
          bestVariance = variance
        }
      }
    }

    val iterator = values.iterator()
    while (iterator.hasNext) {
      val value = iterator.next()
      // Secondary sort đảm bảo mọi value của một size đứng liền nhau.
      if (currentSize != null && value.size != currentSize) {
        finishSize()
        purchaseCount = 0L
        amountCount = 0L
        amountSum = 0.0
        amountSquareSum = 0.0
      }
      currentSize = value.size
      purchaseCount += value.purchaseCount
      amountCount += value.amountCount
      amountSum += value.amountSum
      amountSquareSum += value.amountSquareSum
    }
    finishSize()

    val varianceText = bestVariance
      .map(value => String.format(Locale.ROOT, "%.6f", Double.box(value)))
      .getOrElse("")
    output.set(s"${key.state},${LocalDate.ofEpochDay(key.windowDateEpochDay)},$bestSize,$bestPurchaseCount,$varianceText")
    context.write(NullWritable.get(), output)
    context.getCounter(Task11Constants.CounterGroup, "OUTPUT_ROWS").increment(1L)
  }

  // Variance không xác định (mọi Amount đều null) đứng sau variance xác định.
  private def compareVariance(left: Option[Double], right: Option[Double]): Int = (left, right) match {
    case (Some(a), Some(b)) => java.lang.Double.compare(a, b)
    case (Some(_), None)    => -1
    case (None, Some(_))    => 1
    case (None, None)       => 0
  }
}

/** Driver nối Job 0 và Job 1, sau đó đưa part-file từ HDFS ra filesystem thường. */
final class Task11Job extends Configured with Tool {
  override def run(args: Array[String]): Int = {
    require(args.length == 3, "Usage: Task11 <hdfs-input.csv> <hdfs-work-dir> <local-output.csv>")
    val startedAt = System.nanoTime()
    val input = new HadoopPath(args(0))
    val workRoot = new HadoopPath(args(1))
    require(workRoot.depth() >= 2, s"Refusing an overly broad work directory: $workRoot")

    val stateCountOutput = new HadoopPath(workRoot, "state-counts")
    val resultOutput = new HadoopPath(workRoot, "result")
    val hdfs = workRoot.getFileSystem(getConf)

    // Hadoop không cho ghi vào output directory đã tồn tại. Chỉ xóa workRoot đã
    // được kiểm tra depth >= 2; input và file kết quả local không nằm trong đây.
    if (hdfs.exists(workRoot)) hdfs.delete(workRoot, true)

    // Job 0 phải hoàn tất trước vì output của nó là input nhỏ cho Job 1.
    val countJob = createStateCountJob(input, stateCountOutput)
    if (!countJob.waitForCompletion(true)) return 1
    val boughtRecords = countJob.getCounters
      .findCounter(Task11Constants.CounterGroup, "BOUGHT_RECORDS")
      .getValue

    val countPart = new HadoopPath(stateCountOutput, "part-r-00000")
    // Fragment #task11-state-counts.tsv tạo tên symlink ổn định trong mapper.
    val bucketJob = createWindowJob(input, resultOutput, qualifiedCacheUri(hdfs, countPart))
    if (!bucketJob.waitForCompletion(true)) return 2

    val counters = bucketJob.getCounters
    val rejectedRecords = counters.findCounter(Task11Constants.CounterGroup, "INVALID_TASK_FIELDS").getValue
    val malformedRecords = counters.findCounter(Task11Constants.CounterGroup, "WINDOW_MALFORMED_CSV").getValue
    val outputRows = counters.findCounter(Task11Constants.CounterGroup, "OUTPUT_ROWS").getValue

    copyResultToLocal(hdfs, new HadoopPath(resultOutput, "part-r-00000"), args(2))
    val elapsedSeconds = (System.nanoTime() - startedAt) / 1e9
    println(f"Task 1-1 complete in $elapsedSeconds%.3f seconds")
    println(s"Input: ${args(0)}")
    println(s"Output: ${Paths.get(args(2)).toAbsolutePath.normalize()}")
    println(s"Bought records: $boughtRecords")
    println(s"Rejected task records: $rejectedRecords")
    println(s"Malformed CSV records: $malformedRecords")
    println(s"Output rows: $outputRows")
    0
  }

  private def createStateCountJob(input: HadoopPath, output: HadoopPath): Job = {
    val job = Job.getInstance(getConf, "lab3-task11-state-bought-counts")
    job.setJarByClass(classOf[Task11Job])
    job.setMapperClass(classOf[StateBoughtCountMapper])
    job.setCombinerClass(classOf[LongSumReducer])
    job.setReducerClass(classOf[LongSumReducer])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[LongWritable])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[LongWritable])
    // Bảng state rất nhỏ và một reducer giúp tạo đúng một file cache.
    job.setNumReduceTasks(1)
    FileInputFormat.addInputPath(job, input)
    FileOutputFormat.setOutputPath(job, output)
    job
  }

  private def createWindowJob(input: HadoopPath, output: HadoopPath, cacheUri: URI): Job = {
    val job = Job.getInstance(getConf, "lab3-task11-dynamic-sliding-window")
    job.setJarByClass(classOf[Task11Job])
    job.addCacheFile(cacheUri)
    job.setMapperClass(classOf[WindowBucketMapper])
    job.setCombinerClass(classOf[WindowMomentsCombiner])
    // Combiner nhóm full key; reducer nhóm state/ngày. Hai comparator phục vụ
    // hai mục đích khác nhau nên phải cấu hình tách biệt.
    job.setCombinerKeyGroupingComparatorClass(classOf[FullKeyGroupingComparator])
    job.setPartitionerClass(classOf[StateDatePartitioner])
    job.setGroupingComparatorClass(classOf[StateDateGroupingComparator])
    job.setReducerClass(classOf[WindowWinnerReducer])
    job.setMapOutputKeyClass(classOf[WindowSizeKey])
    job.setMapOutputValueClass(classOf[WindowMoments])
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Text])
    // Đề yêu cầu một physical CSV file. Một reducer cũng giữ output sort ổn định.
    job.setNumReduceTasks(1)
    FileInputFormat.addInputPath(job, input)
    FileOutputFormat.setOutputPath(job, output)
    job
  }

  private def qualifiedCacheUri(fileSystem: FileSystem, path: HadoopPath): URI = {
    val qualified = fileSystem.makeQualified(path).toUri.toString
    new URI(s"$qualified#${Task11Constants.CacheAlias}")
  }

  private def copyResultToLocal(hdfs: FileSystem, source: HadoopPath, outputPath: String): Unit = {
    val target = Paths.get(outputPath).toAbsolutePath.normalize()
    Option(target.getParent).foreach(parent => Files.createDirectories(parent))
    val temporary = target.resolveSibling(s".${target.getFileName}.tmp")
    val input = hdfs.open(source)
    try Files.copy(input, temporary, StandardCopyOption.REPLACE_EXISTING)
    finally input.close()

    // Ghi qua file tạm rồi rename để tránh để lại file kết quả dở dang nếu copy lỗi.
    try Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
    catch {
      case _: java.nio.file.AtomicMoveNotSupportedException =>
        Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING)
    }
  }
}

object Task11 {
  def main(args: Array[String]): Unit = System.exit(ToolRunner.run(new Task11Job(), args))
}
