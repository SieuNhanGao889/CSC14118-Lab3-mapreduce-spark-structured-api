package lab3.task12

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.{Text, Writable, WritableComparable, WritableComparator}
import org.apache.hadoop.mapreduce.Partitioner

/**
 * Key trung gian của bước tính variety: (state, month, style, sku).
 *
 * Hadoop sort theo toàn bộ key nên các SKU của cùng một style nằm cạnh nhau.
 * Reducer sau đó chỉ group theo (state, month, style), nhờ vậy một lần reduce có
 * thể vừa đếm distinct SKU vừa biết style có từng phục vụ size >= XXL hay không.
 */
final class StyleSkuKey extends WritableComparable[StyleSkuKey] {
  private val stateValue = new Text()
  private val monthValue = new Text()
  private val styleValue = new Text()
  private val skuValue = new Text()

  def state: String = stateValue.toString
  def month: String = monthValue.toString
  def style: String = styleValue.toString
  def sku: String = skuValue.toString

  def set(state: String, month: String, style: String, sku: String): Unit = {
    stateValue.set(state)
    monthValue.set(month)
    styleValue.set(style)
    skuValue.set(sku)
  }

  override def write(out: DataOutput): Unit = {
    // readFields phải đọc lại đúng thứ tự này.
    stateValue.write(out)
    monthValue.write(out)
    styleValue.write(out)
    skuValue.write(out)
  }

  override def readFields(in: DataInput): Unit = {
    stateValue.readFields(in)
    monthValue.readFields(in)
    styleValue.readFields(in)
    skuValue.readFields(in)
  }

  override def compareTo(other: StyleSkuKey): Int = {
    val byState = stateValue.compareTo(other.stateValue)
    if (byState != 0) return byState
    val byMonth = monthValue.compareTo(other.monthValue)
    if (byMonth != 0) return byMonth
    val byStyle = styleValue.compareTo(other.styleValue)
    if (byStyle != 0) return byStyle
    skuValue.compareTo(other.skuValue)
  }

  override def hashCode(): Int = {
    var result = stateValue.hashCode()
    result = 31 * result + monthValue.hashCode()
    result = 31 * result + styleValue.hashCode()
    31 * result + skuValue.hashCode()
  }

  override def equals(other: Any): Boolean = other match {
    case key: StyleSkuKey => compareTo(key) == 0
    case _                => false
  }
}

/**
 * Value đi cùng StyleSkuKey.
 *
 * SKU được lặp lại trong value vì reducer grouping đã bỏ phần SKU khỏi key mà
 * reducer nhìn thấy. sizeRank dùng để lấy size lớn nhất của style trong nhóm.
 */
final class SkuSizeObservation extends Writable {
  private val skuValue = new Text()
  private var sizeRankValue = 0

  def sku: String = skuValue.toString
  def sizeRank: Int = sizeRankValue

  def set(sku: String, sizeRank: Int): Unit = {
    skuValue.set(sku)
    sizeRankValue = sizeRank
  }

  override def write(out: DataOutput): Unit = {
    skuValue.write(out)
    out.writeInt(sizeRankValue)
  }

  override def readFields(in: DataInput): Unit = {
    skuValue.readFields(in)
    sizeRankValue = in.readInt()
  }
}

/** Key của bước tính median: một state trong một tháng. */
final class StateMonthKey extends WritableComparable[StateMonthKey] {
  private val stateValue = new Text()
  private val monthValue = new Text()

  def state: String = stateValue.toString
  def month: String = monthValue.toString

  def set(state: String, month: String): Unit = {
    stateValue.set(state)
    monthValue.set(month)
  }

  override def write(out: DataOutput): Unit = {
    stateValue.write(out)
    monthValue.write(out)
  }

  override def readFields(in: DataInput): Unit = {
    stateValue.readFields(in)
    monthValue.readFields(in)
  }

  override def compareTo(other: StateMonthKey): Int = {
    val byState = stateValue.compareTo(other.stateValue)
    if (byState != 0) byState else monthValue.compareTo(other.monthValue)
  }

  override def hashCode(): Int = 31 * stateValue.hashCode() + monthValue.hashCode()

  override def equals(other: Any): Boolean = other match {
    case key: StateMonthKey => compareTo(key) == 0
    case _                  => false
  }
}

/**
 * Kết quả của một style trước khi tính median.
 * locallyEligible cho biết style đó có size >= XXL ngay trong state-month này.
 * Cờ này luôn true ở chế độ local; ở chế độ global nó phục vụ sensitivity report.
 */
final class VarietyObservation extends Writable {
  private var varietyValue = 0
  private var locallyEligibleValue = false

  def variety: Int = varietyValue
  def locallyEligible: Boolean = locallyEligibleValue

  def set(variety: Int, locallyEligible: Boolean): Unit = {
    varietyValue = variety
    locallyEligibleValue = locallyEligible
  }

  override def write(out: DataOutput): Unit = {
    out.writeInt(varietyValue)
    out.writeBoolean(locallyEligibleValue)
  }

  override def readFields(in: DataInput): Unit = {
    varietyValue = in.readInt()
    locallyEligibleValue = in.readBoolean()
  }
}

/** Mọi SKU của cùng (state, month, style) phải đến cùng reducer. */
final class StateMonthStylePartitioner extends Partitioner[StyleSkuKey, SkuSizeObservation] {
  override def getPartition(key: StyleSkuKey, value: SkuSizeObservation, partitions: Int): Int = {
    var hash = key.state.hashCode
    hash = 31 * hash + key.month.hashCode
    hash = 31 * hash + key.style.hashCode
    (hash & Int.MaxValue) % partitions
  }
}

/** Reducer group theo style, nhưng dữ liệu bên trong vẫn sort theo SKU. */
final class StateMonthStyleGroupingComparator
    extends WritableComparator(classOf[StyleSkuKey], true) {
  override def compare(left: WritableComparable[_], right: WritableComparable[_]): Int = {
    val a = left.asInstanceOf[StyleSkuKey]
    val b = right.asInstanceOf[StyleSkuKey]
    val byState = a.state.compareTo(b.state)
    if (byState != 0) return byState
    val byMonth = a.month.compareTo(b.month)
    if (byMonth != 0) return byMonth
    a.style.compareTo(b.style)
  }
}

/** Combiner chỉ được gộp những record có cùng full key, bao gồm cả SKU. */
final class FullStyleSkuGroupingComparator
    extends WritableComparator(classOf[StyleSkuKey], true) {
  override def compare(left: WritableComparable[_], right: WritableComparable[_]): Int =
    left.asInstanceOf[StyleSkuKey].compareTo(right.asInstanceOf[StyleSkuKey])
}
