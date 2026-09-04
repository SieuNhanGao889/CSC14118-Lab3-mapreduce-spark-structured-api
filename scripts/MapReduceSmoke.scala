package lab3.tools

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.{Tool, ToolRunner}

/** Kiểm thử hạ tầng Hadoop Mapreduct của local trước khi chạy lab3
  * PhysicalLineMapper: đọc từng dòng của file input và phát ra cặp ("physical_lines", 1)
  * LongSumReducer: tổng hợp số dòng từ các Mapper nhận toàn bộ giá trị 1 thuộc key "physical_lines" và phát ra cặp ("physical_lines", tổng số dòng)
  * MapReduceSmokeJob: Gán LongSumReducer làm Combiner giới hạn output về 1 partition duy nhất, kết quả cuối cùng là 1 file part-00000 chứa tổng số dòng của file input.
  * MapReduceSmoke: object chứa hàm main để chạy MapReduceSmokeJob với các tham số đầu vào và đầu ra từ command line.
  */
class PhysicalLineMapper extends Mapper[LongWritable, Text, Text, LongWritable] {
  private val metric = new Text("physical_lines")
  private val one = new LongWritable(1L)

  override def map(
      key: LongWritable,
      value: Text,
      context: Mapper[LongWritable, Text, Text, LongWritable]#Context
  ): Unit = context.write(metric, one)
}

class LongSumReducer extends Reducer[Text, LongWritable, Text, LongWritable] {
  private val result = new LongWritable()

  override def reduce(
      key: Text,
      values: java.lang.Iterable[LongWritable],
      context: Reducer[Text, LongWritable, Text, LongWritable]#Context
  ): Unit = {
    val iterator = values.iterator()
    var total = 0L
    while (iterator.hasNext) total += iterator.next().get()
    result.set(total)
    context.write(key, result)
  }
}

/** Minimal MapReduce job used only to prove HDFS/YARN/JAR execution. */
class MapReduceSmokeJob extends Configured with Tool {
  override def run(args: Array[String]): Int = {
    require(args.length == 2, "Usage: MapReduceSmoke <input> <output>")
    val job = Job.getInstance(getConf, "lab3-mapreduce-smoke")
    job.setJarByClass(getClass)
    job.setMapperClass(classOf[PhysicalLineMapper])
    job.setCombinerClass(classOf[LongSumReducer])
    job.setReducerClass(classOf[LongSumReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[LongWritable])
    job.setNumReduceTasks(1)
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    if (job.waitForCompletion(true)) 0 else 1
  }

}

object MapReduceSmoke {
  def main(args: Array[String]): Unit = System.exit(ToolRunner.run(new MapReduceSmokeJob(), args))
}
