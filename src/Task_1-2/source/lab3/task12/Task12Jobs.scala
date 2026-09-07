package lab3.task12

import java.io.File
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util.Locale

import lab3.common.{AmazonCsv, LabRules}
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.{Tool, ToolRunner}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

private object Task12Constants {
  val CounterGroup = "Task12"
  val ModeKey = "lab3.task12.eligibility.mode"
  val LocalMode = "local"
  val GlobalMode = "global"
  val DefaultMode = GlobalMode
  val EligibleStyleCacheAlias = "task12-global-eligible-styles.txt"
  val CsvHeader = "state,month,median_variety,eligible_style_count,eligibility_scope"
}

/**
 * Job chuẩn bị chỉ dùng cho mode "global" (mode nộp chính).
 * Mỗi record có size rank >= XXL phát style của nó; reducer loại trùng để tạo
 * tập style từng phục vụ size lớn ở bất kỳ state/tháng nào trong dataset.
 * output value là NullWritable vì chỉ cần emit key thôi; reducer cũng không cần biết value.
 * so sánh với StateBoughtCountMapper ở Task 1-1: cần đếm tổng số => khác với global job chỉ cần yes/no style.
 */
final class GlobalEligibleStyleMapper
    extends Mapper[org.apache.hadoop.io.LongWritable, Text, Text, NullWritable] {
  private val outputStyle = new Text()

  override def map(
      offset: org.apache.hadoop.io.LongWritable,
      value: Text,
      context: Mapper[org.apache.hadoop.io.LongWritable, Text, Text, NullWritable]#Context
  ): Unit = {
    val line = value.toString
    if (offset.get() == 0L && AmazonCsv.isHeader(line)) return

    AmazonCsv.parse(line) match {
      case Left(_) =>
        context.getCounter(Task12Constants.CounterGroup, "ELIGIBILITY_MALFORMED_CSV").increment(1L)
      // style và sizerank của record tồn tại và guard rule thỏa -> set style vào output key, value là NullWritable.
      case Right(record) =>
        (record.style, record.sizeRank) match {
          case (Some(style), Some(rank)) if rank >= LabRules.MinLargeSizeRank =>
            outputStyle.set(style)
            context.write(outputStyle, NullWritable.get())
            context.getCounter(Task12Constants.CounterGroup, "LARGE_SIZE_OCCURRENCES").increment(1L)
          case _ => ()
        }
    }
  }
}

/** Combiner loại style trùng ngay tại mapper, chưa ghi counter kết quả cuối.*/
final class DistinctStyleCombiner extends Reducer[Text, NullWritable, Text, NullWritable] {
  override def reduce(
      style: Text,
      ignored: java.lang.Iterable[NullWritable],
      context: Reducer[Text, NullWritable, Text, NullWritable]#Context
  ): Unit = context.write(style, NullWritable.get())
}

/** Reducer loại trùng toàn cục; counter này vì vậy đúng bằng số style phân biệt. */
final class DistinctStyleReducer extends Reducer[Text, NullWritable, Text, NullWritable] {
  override def reduce(
      style: Text,
      ignored: java.lang.Iterable[NullWritable],
      context: Reducer[Text, NullWritable, Text, NullWritable]#Context
  ): Unit = {
    context.write(style, NullWritable.get())
    context.getCounter(Task12Constants.CounterGroup, "GLOBAL_ELIGIBLE_STYLES").increment(1L)
  }
}

/**
 * Mapper của bước tính variety.
 *
 * Không lọc Shipped/Qty: Task 1-2 định nghĩa variety theo các record/SKU xuất
 * hiện trong state-month, không tái sử dụng định nghĩa "bought" riêng của Task 1-1.
 * State, Date, Style và SKU thiếu thì record không thể tham gia nhóm và bị đếm lỗi.
 */
final class StyleSkuMapper
    extends Mapper[org.apache.hadoop.io.LongWritable, Text, StyleSkuKey, SkuSizeObservation] {
  private val outputKey = new StyleSkuKey()
  private val outputValue = new SkuSizeObservation()
  private var mode = Task12Constants.DefaultMode
  private var globallyEligibleStyles = Set.empty[String]

  override def setup(context: Mapper[org.apache.hadoop.io.LongWritable, Text, StyleSkuKey, SkuSizeObservation]#Context): Unit = {
    mode = context.getConfiguration.get(Task12Constants.ModeKey, Task12Constants.DefaultMode)

    // Local mode không cần cache. Global mode đọc tập style nhỏ do job phụ tạo.
    if (mode == Task12Constants.GlobalMode) {
      val cacheFile = new File(Task12Constants.EligibleStyleCacheAlias)
      if (!cacheFile.isFile)
        throw new IllegalStateException(s"Missing eligible-style cache: ${cacheFile.getAbsolutePath}")
      val source = Source.fromFile(cacheFile, StandardCharsets.UTF_8.name())
      try globallyEligibleStyles = source.getLines().map(_.trim).filter(_.nonEmpty).toSet
      finally source.close()
    }
  }

  override def map(
      offset: org.apache.hadoop.io.LongWritable,
      value: Text,
      context: Mapper[org.apache.hadoop.io.LongWritable, Text, StyleSkuKey, SkuSizeObservation]#Context
  ): Unit = {
    val line = value.toString
    if (offset.get() == 0L && AmazonCsv.isHeader(line)) return
    context.getCounter(Task12Constants.CounterGroup, "INPUT_RECORDS").increment(1L)

    AmazonCsv.parse(line) match {
      case Left(_) =>
        context.getCounter(Task12Constants.CounterGroup, "MALFORMED_CSV").increment(1L)
      case Right(record) =>
        (record.shipState.filter(_ => record.hasValidState), record.orderMonth, record.style, record.sku) match {
          case (Some(state), Some(month), Some(style), Some(sku)) =>
            // Global mode có thể loại style ngay tại mapper để giảm shuffle.
            if (mode == Task12Constants.LocalMode || globallyEligibleStyles.contains(style)) {
              outputKey.set(state, month, style, sku)
              outputValue.set(sku, record.sizeRank.getOrElse(0))
              context.write(outputKey, outputValue)
              context.getCounter(Task12Constants.CounterGroup, "STYLE_SKU_RECORDS_EMITTED").increment(1L)
            } else {
              context.getCounter(Task12Constants.CounterGroup, "GLOBAL_INELIGIBLE_RECORDS").increment(1L)
            }
          case _ =>
            context.getCounter(Task12Constants.CounterGroup, "INVALID_TASK_FIELDS").increment(1L)
        }
    }
  }
}

/**
 * Gộp các record trùng full key (state, month, style, sku) ngay tại mapper.
 * Chỉ cần giữ sizeRank lớn nhất vì reducer chỉ quan tâm style có chạm rank 6 không.
 */
final class StyleSkuCombiner
    extends Reducer[StyleSkuKey, SkuSizeObservation, StyleSkuKey, SkuSizeObservation] {
  private val combined = new SkuSizeObservation()

  override def reduce(
      key: StyleSkuKey,
      values: java.lang.Iterable[SkuSizeObservation],
      context: Reducer[StyleSkuKey, SkuSizeObservation, StyleSkuKey, SkuSizeObservation]#Context
  ): Unit = {
    val iterator = values.iterator()
    var maxSizeRank = 0
    while (iterator.hasNext) maxSizeRank = math.max(maxSizeRank, iterator.next().sizeRank)
    combined.set(key.sku, maxSizeRank)
    context.write(key, combined)
  }
}

/**
 * Một lần reduce nhận toàn bộ SKU đã sort của một (state, month, style).
 * Ta đếm lúc SKU thay đổi thay vì tạo HashSet, nên bộ nhớ dùng là O(1).
 */
final class StyleVarietyReducer
    extends Reducer[StyleSkuKey, SkuSizeObservation, StateMonthKey, VarietyObservation] {
  private val outputKey = new StateMonthKey()
  private val outputValue = new VarietyObservation()
  private var mode = Task12Constants.DefaultMode

  override def setup(context: Reducer[StyleSkuKey, SkuSizeObservation, StateMonthKey, VarietyObservation]#Context): Unit =
    mode = context.getConfiguration.get(Task12Constants.ModeKey, Task12Constants.DefaultMode)

  override def reduce(
      key: StyleSkuKey,
      values: java.lang.Iterable[SkuSizeObservation],
      context: Reducer[StyleSkuKey, SkuSizeObservation, StateMonthKey, VarietyObservation]#Context
  ): Unit = {
    val iterator = values.iterator()
    var previousSku: String = null
    var distinctSkuCount = 0
    var maxSizeRank = 0

    while (iterator.hasNext) {
      val observation = iterator.next()
      if (previousSku == null || observation.sku != previousSku) {
        distinctSkuCount += 1
        previousSku = observation.sku
      }
      maxSizeRank = math.max(maxSizeRank, observation.sizeRank)
    }

    val locallyEligible = maxSizeRank >= LabRules.MinLargeSizeRank
    // Local: chỉ style có size >= XXL ngay trong nhóm mới đi tiếp.
    // Global: mapper đã lọc bằng tập global, nên mọi style ở đây đều đi tiếp.
    if (mode == Task12Constants.GlobalMode || locallyEligible) {
      outputKey.set(key.state, key.month)
      outputValue.set(distinctSkuCount, locallyEligible)
      context.write(outputKey, outputValue)
      context.getCounter(Task12Constants.CounterGroup, "STYLE_VARIETY_ROWS").increment(1L)
    }
  }
}

/** SequenceFile đã có đúng key/value nên mapper chỉ chuyển tiếp dữ liệu. */
final class VarietyIdentityMapper
    extends Mapper[StateMonthKey, VarietyObservation, StateMonthKey, VarietyObservation] {
  override def map(
      key: StateMonthKey,
      value: VarietyObservation,
      context: Mapper[StateMonthKey, VarietyObservation, StateMonthKey, VarietyObservation]#Context
  ): Unit = context.write(key, value)
}

/**
 * Reducer cuối: sort danh sách variety và lấy median cho từng state-month.
 * Nhóm lớn nhất chỉ vài trăm style nên giữ ArrayBuffer trong bộ nhớ là hợp lý.
 */
final class MedianVarietyReducer
    extends Reducer[StateMonthKey, VarietyObservation, NullWritable, Text] {
  private val output = new Text()
  private var mode = Task12Constants.DefaultMode

  override def setup(context: Reducer[StateMonthKey, VarietyObservation, NullWritable, Text]#Context): Unit = {
    mode = context.getConfiguration.get(Task12Constants.ModeKey, Task12Constants.DefaultMode)
    // Một reducer duy nhất nên header chỉ xuất hiện đúng một lần.
    output.set(Task12Constants.CsvHeader)
    context.write(NullWritable.get(), output)
  }

  override def reduce(
      key: StateMonthKey,
      values: java.lang.Iterable[VarietyObservation],
      context: Reducer[StateMonthKey, VarietyObservation, NullWritable, Text]#Context
  ): Unit = {
    val varieties = ArrayBuffer.empty[Int]
    val iterator = values.iterator()
    while (iterator.hasNext) {
      val observation = iterator.next()
      varieties += observation.variety
    }

    // Local mode đã lọc style theo chính state-month ở Job 1. Global mode phải
    // xuất mọi state-month có globally eligible style; không giới hạn về 128
    // nhóm local, nếu không đây chỉ là phép so sánh trên giao hai miền chứ chưa
    // phải kết quả global đầy đủ.
    if (varieties.nonEmpty) {
      val sorted = varieties.sorted
      val middle = sorted.length / 2
      val median = if (sorted.length % 2 == 1) sorted(middle).toDouble
      else (sorted(middle - 1).toDouble + sorted(middle).toDouble) / 2.0

      output.set(String.format(
        Locale.ROOT,
        "%s,%s,%.1f,%d,%s",
        key.state,
        key.month,
        Double.box(median),
        Int.box(sorted.length),
        mode
      ))
      context.write(NullWritable.get(), output)
      context.getCounter(Task12Constants.CounterGroup, "OUTPUT_GROUPS").increment(1L)
    }
  }
}

/** Điều phối các MapReduce job và xuất part-file cuối về filesystem thường. */
final class Task12Job extends Configured with Tool {
  override def run(args: Array[String]): Int = {
    require(args.length == 3 || args.length == 4,
      "Usage: Task12 <hdfs-input.csv> <hdfs-work-dir> <local-output.csv> [local|global, default=global]")

    val startedAt = System.nanoTime()
    val input = new HadoopPath(args(0))
    val workRoot = new HadoopPath(args(1))
    val localOutput = args(2)
    // Mặc định dùng global.
    // Local vẫn được giữ như sensitivity mode và phải truyền tường minh "local".
    val mode = args.lift(3).getOrElse(Task12Constants.DefaultMode).toLowerCase(Locale.ROOT)
    require(Set(Task12Constants.LocalMode, Task12Constants.GlobalMode).contains(mode),
      s"Eligibility mode must be local or global, found: $mode")
    require(workRoot.depth() >= 2, s"Refusing an overly broad work directory: $workRoot")

    val hdfs = workRoot.getFileSystem(getConf)
    if (hdfs.exists(workRoot)) hdfs.delete(workRoot, true)

    // Global mode cần job phụ và cache; local mode đi thẳng vào bước variety.
    val eligibleStyleCache = if (mode == Task12Constants.GlobalMode) {
      val output = new HadoopPath(workRoot, "global-eligible-styles")
      val job = createGlobalEligibilityJob(input, output)
      if (!job.waitForCompletion(true)) return 1
      Some(qualifiedCacheUri(hdfs, new HadoopPath(output, "part-r-00000")))
    } else None

    val varietyOutput = new HadoopPath(workRoot, "style-varieties")
    val varietyJob = createVarietyJob(input, varietyOutput, mode, eligibleStyleCache)
    if (!varietyJob.waitForCompletion(true)) return 2
    val styleVarietyRows = varietyJob.getCounters
      .findCounter(Task12Constants.CounterGroup, "STYLE_VARIETY_ROWS").getValue

    val medianOutput = new HadoopPath(workRoot, "median-result")
    val medianJob = createMedianJob(varietyOutput, medianOutput, mode)
    if (!medianJob.waitForCompletion(true)) return 3
    val outputGroups = medianJob.getCounters
      .findCounter(Task12Constants.CounterGroup, "OUTPUT_GROUPS").getValue

    copyResultToLocal(hdfs, new HadoopPath(medianOutput, "part-r-00000"), localOutput)
    val elapsedSeconds = (System.nanoTime() - startedAt) / 1e9
    println(f"Task 1-2 ($mode eligibility) complete in $elapsedSeconds%.3f seconds")
    println(s"Input: ${args(0)}")
    println(s"Output: ${Paths.get(localOutput).toAbsolutePath.normalize()}")
    println(s"Eligible style rows before median: $styleVarietyRows")
    println(s"Output state-month groups: $outputGroups")
    0
  }

  private def createGlobalEligibilityJob(input: HadoopPath, output: HadoopPath): Job = {
    val job = Job.getInstance(getConf, "lab3-task12-global-eligible-styles")
    job.setJarByClass(classOf[Task12Job])
    job.setMapperClass(classOf[GlobalEligibleStyleMapper])
    job.setCombinerClass(classOf[DistinctStyleCombiner])
    job.setReducerClass(classOf[DistinctStyleReducer])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[NullWritable])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[NullWritable])
    // Một reducer tạo đúng một file nhỏ để Distributed Cache phân phát.
    job.setNumReduceTasks(1)
    FileInputFormat.addInputPath(job, input)
    FileOutputFormat.setOutputPath(job, output)
    job
  }

  private def createVarietyJob(
      input: HadoopPath,
      output: HadoopPath,
      mode: String,
      cacheUri: Option[URI]
  ): Job = {
    val job = Job.getInstance(getConf, s"lab3-task12-style-variety-$mode")
    job.getConfiguration.set(Task12Constants.ModeKey, mode)
    job.setJarByClass(classOf[Task12Job])
    cacheUri.foreach(job.addCacheFile)
    job.setMapperClass(classOf[StyleSkuMapper])
    job.setCombinerClass(classOf[StyleSkuCombiner])
    job.setCombinerKeyGroupingComparatorClass(classOf[FullStyleSkuGroupingComparator])
    job.setPartitionerClass(classOf[StateMonthStylePartitioner])
    job.setGroupingComparatorClass(classOf[StateMonthStyleGroupingComparator])
    job.setReducerClass(classOf[StyleVarietyReducer])
    job.setMapOutputKeyClass(classOf[StyleSkuKey])
    job.setMapOutputValueClass(classOf[SkuSizeObservation])
    job.setOutputKeyClass(classOf[StateMonthKey])
    job.setOutputValueClass(classOf[VarietyObservation])
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[StateMonthKey, VarietyObservation]])
    // Dữ liệu chỉ 129k dòng; một reducer giữ intermediate deterministic và đơn giản.
    job.setNumReduceTasks(1)
    FileInputFormat.addInputPath(job, input)
    FileOutputFormat.setOutputPath(job, output)
    job
  }

  private def createMedianJob(input: HadoopPath, output: HadoopPath, mode: String): Job = {
    val job = Job.getInstance(getConf, s"lab3-task12-median-variety-$mode")
    job.getConfiguration.set(Task12Constants.ModeKey, mode)
    job.setJarByClass(classOf[Task12Job])
    job.setInputFormatClass(classOf[SequenceFileInputFormat[StateMonthKey, VarietyObservation]])
    job.setMapperClass(classOf[VarietyIdentityMapper])
    job.setReducerClass(classOf[MedianVarietyReducer])
    job.setMapOutputKeyClass(classOf[StateMonthKey])
    job.setMapOutputValueClass(classOf[VarietyObservation])
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Text])
    // Một reducer để file CSV có đúng một header và thứ tự output ổn định.
    job.setNumReduceTasks(1)
    FileInputFormat.addInputPath(job, input)
    FileOutputFormat.setOutputPath(job, output)
    job
  }

  private def qualifiedCacheUri(fileSystem: FileSystem, path: HadoopPath): URI = {
    val qualified = fileSystem.makeQualified(path).toUri.toString
    new URI(s"$qualified#${Task12Constants.EligibleStyleCacheAlias}")
  }

  private def copyResultToLocal(hdfs: FileSystem, source: HadoopPath, outputPath: String): Unit = {
    val target = Paths.get(outputPath).toAbsolutePath.normalize()
    Option(target.getParent).foreach(parent => Files.createDirectories(parent))
    val temporary = target.resolveSibling(s".${target.getFileName}.tmp")
    val input = hdfs.open(source)
    try Files.copy(input, temporary, StandardCopyOption.REPLACE_EXISTING)
    finally input.close()

    // Rename file tạm ở cuối để tránh để lại output dở dang nếu copy thất bại.
    try Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
    catch {
      case _: java.nio.file.AtomicMoveNotSupportedException =>
        Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING)
    }
  }
}

object Task12 {
  def main(args: Array[String]): Unit = System.exit(ToolRunner.run(new Task12Job(), args))
}
