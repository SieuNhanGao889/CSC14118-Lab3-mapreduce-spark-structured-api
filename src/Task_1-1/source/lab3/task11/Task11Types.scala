package lab3.task11

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.{Text, Writable, WritableComparable, WritableComparator}
import org.apache.hadoop.mapreduce.Partitioner

/**
 * Composite key của Job 1: (state, ngày cửa sổ, size).
 *
 * Hadoop yêu cầu key trung gian phải triển khai WritableComparable để:
 *   1. serialize key trước khi shuffle;
 *   2. sort key theo state -> ngày -> size.
 *
 * Đây là class mutable, không phải case class, vì Hadoop sẽ tái sử dụng object rất
 * nhiều lần để giảm lượng object và áp lực garbage collection trong mapper/reducer.
 * Các hàm: 
 *  - set() để gán giá trị cho key
 *  - compareTo() để so sánh key theo thứ tự state -> ngày -> size
 *  - hashCode() và equals() để so sánh key trong các collection
 *  - write() và readFields() để serialize/deserialize key
 *  - equals() để so sánh key trong các collection
 */
final class WindowSizeKey extends WritableComparable[WindowSizeKey] {
  private val stateValue = new Text()
  private val sizeValue = new Text()
  private var windowDateEpochDayValue = 0L 

  def state: String = stateValue.toString
  def size: String = sizeValue.toString
  def windowDateEpochDay: Long = windowDateEpochDayValue

  def set(state: String, windowDateEpochDay: Long, size: String): Unit = {
    stateValue.set(state)
    windowDateEpochDayValue = windowDateEpochDay
    sizeValue.set(size)
  }

  override def write(out: DataOutput): Unit = {
    // Thứ tự ghi phải giống hoàn toàn thứ tự đọc trong readFields.
    stateValue.write(out)
    out.writeLong(windowDateEpochDayValue)
    sizeValue.write(out)
  }

  override def readFields(in: DataInput): Unit = {
    stateValue.readFields(in)
    windowDateEpochDayValue = in.readLong()
    sizeValue.readFields(in)
  }

  override def compareTo(other: WindowSizeKey): Int = {
    // Sort theo full key. Nhờ size đứng cuối, các size trong cùng state/ngày
    // tới reducer theo thứ tự từ điển, phục vụ tie-break cuối cùng của đề.
    val byState = stateValue.compareTo(other.stateValue)
    if (byState != 0) byState
    else {
      val byDate = java.lang.Long.compare(windowDateEpochDayValue, other.windowDateEpochDayValue)
      if (byDate != 0) byDate else sizeValue.compareTo(other.sizeValue)
    }
  }

  override def hashCode(): Int = {
    var result = stateValue.hashCode()
    result = 31 * result + java.lang.Long.hashCode(windowDateEpochDayValue)
    31 * result + sizeValue.hashCode()
  }

  override def equals(other: Any): Boolean = other match {
    case key: WindowSizeKey => compareTo(key) == 0
    case _                  => false
  }
}

/**
 * Thống kê cộng dồn cho một size trong một cửa sổ.
 *
 * purchaseCount dùng để tìm size xuất hiện nhiều nhất. amountCount tách riêng vì
 * Amount có thể null: record đó vẫn được tính tần suất nhưng không tham gia variance.
 * Bốn đại lượng này đều cộng được trong Combiner:
 *
 *   mean     = amountSum / amountCount
 *   variance = amountSquareSum / amountCount - mean²
 */
final class WindowMoments extends Writable {
  private val sizeValue = new Text()
  private var purchaseCountValue = 0L
  private var amountCountValue = 0L
  private var amountSumValue = 0.0
  private var amountSquareSumValue = 0.0

  def size: String = sizeValue.toString
  def purchaseCount: Long = purchaseCountValue
  def amountCount: Long = amountCountValue
  def amountSum: Double = amountSumValue
  def amountSquareSum: Double = amountSquareSumValue

  def set(size: String, purchaseCount: Long, amountCount: Long, amountSum: Double, amountSquareSum: Double): Unit = {
    sizeValue.set(size)
    purchaseCountValue = purchaseCount
    amountCountValue = amountCount
    amountSumValue = amountSum
    amountSquareSumValue = amountSquareSum
  }

  override def write(out: DataOutput): Unit = {
    sizeValue.write(out)
    out.writeLong(purchaseCountValue)
    out.writeLong(amountCountValue)
    out.writeDouble(amountSumValue)
    out.writeDouble(amountSquareSumValue)
  }

  override def readFields(in: DataInput): Unit = {
    sizeValue.readFields(in)
    purchaseCountValue = in.readLong()
    amountCountValue = in.readLong()
    amountSumValue = in.readDouble()
    amountSquareSumValue = in.readDouble()
  }
}

/**
 * Chỉ hash (state, ngày), không hash size. Vì vậy toàn bộ size cạnh tranh trong
 * cùng một cửa sổ chắc chắn được gửi đến cùng reducer.
 */
final class StateDatePartitioner extends Partitioner[WindowSizeKey, WindowMoments] {
  override def getPartition(key: WindowSizeKey, value: WindowMoments, partitions: Int): Int = {
    val hash = 31 * key.state.hashCode + java.lang.Long.hashCode(key.windowDateEpochDay)
    (hash & Int.MaxValue) % partitions
  }
}

/**
 * Reducer grouping chỉ xét (state, ngày). Một lần gọi reduce sẽ nhận tất cả size
 * của đúng một cửa sổ, mặc dù shuffle vẫn sort bằng full key có cả size
 */
final class StateDateGroupingComparator
    extends WritableComparator(classOf[WindowSizeKey], true) {
  override def compare(left: WritableComparable[_], right: WritableComparable[_]): Int = {
    val a = left.asInstanceOf[WindowSizeKey]
    val b = right.asInstanceOf[WindowSizeKey]
    val byState = a.state.compareTo(b.state)
    if (byState != 0) byState else java.lang.Long.compare(a.windowDateEpochDay, b.windowDateEpochDay)
  }
}

/**
 * Combiner phải nhóm bằng full key (state, ngày, size), nếu không các size khác
 * nhau có thể bị cộng nhầm trước khi tới reducer.
 */
final class FullKeyGroupingComparator
    extends WritableComparator(classOf[WindowSizeKey], true) {
  override def compare(left: WritableComparable[_], right: WritableComparable[_]): Int =
    left.asInstanceOf[WindowSizeKey].compareTo(right.asInstanceOf[WindowSizeKey])
}