package lab3.tools

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.LocalDate

import lab3.common.AmazonCsv
import org.apache.commons.csv.CSVFormat

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Using

/**
 * Bộ kiểm tra độc lập cho output Task 1-1 và Task 1-2.
 *
 * Đây không phải lời giải MapReduce thay thế. Chương trình chạy tuần tự trên
 * máy local, dùng collection Scala để dựng lại kết quả tham chiếu, sau đó so
 * toàn bộ key và metric với các CSV do Hadoop tạo. Cách gom nhóm khác hẳn
 * mapper/combiner/secondary-sort/reducer nên có thể phát hiện lỗi ở dataflow.
 *
 * Usage:
 *   Task1Validator <raw.csv> <task-1-1.csv> <task-1-2-global.csv> <task-1-2-local.csv>
 */
object Task1Validator {
  private final case class BoughtObservation(
      state: String,
      date: LocalDate,
      size: String,
      amount: Option[Double]
  )

  private final class Moments {
    var purchaseCount = 0L
    var amountCount = 0L
    var amountSum = 0.0
    var amountSquareSum = 0.0

    def add(amount: Option[Double]): Unit = {
      purchaseCount += 1L
      amount.foreach { value =>
        amountCount += 1L
        amountSum += value
        amountSquareSum += value * value
      }
    }

    def variance: Option[Double] =
      if (amountCount == 0L) None
      else {
        val mean = amountSum / amountCount
        Some(math.max(0.0, amountSquareSum / amountCount - mean * mean))
      }
  }

  private final class StyleStats {
    val skus: mutable.HashSet[String] = mutable.HashSet.empty
    var maxSizeRank = 0

    def add(sku: String, sizeRank: Int): Unit = {
      skus += sku
      maxSizeRank = math.max(maxSizeRank, sizeRank)
    }
  }

  private final case class WindowExpected(
      size: String,
      purchaseCount: Long,
      variance: Option[Double]
  )

  private final case class MedianExpected(
      median: Double,
      eligibleStyleCount: Int
  )

  private final case class ReferenceData(
      inputRecords: Long,
      malformedRecords: Long,
      stateBoughtCounts: Map[String, Long],
      boughtObservations: Vector[BoughtObservation],
      styleStats: Map[(String, String, String), StyleStats],
      globallyEligibleStyles: Set[String]
  )

  def main(args: Array[String]): Unit = {
    require(
      args.length == 4,
      "Usage: Task1Validator <raw.csv> <task-1-1.csv> <task-1-2-global.csv> <task-1-2-local.csv>"
    )

    val rawInput = Paths.get(args(0)).toAbsolutePath.normalize()
    val task11Output = Paths.get(args(1)).toAbsolutePath.normalize()
    val task12GlobalOutput = Paths.get(args(2)).toAbsolutePath.normalize()
    val task12LocalOutput = Paths.get(args(3)).toAbsolutePath.normalize()
    Seq(rawInput, task11Output, task12GlobalOutput, task12LocalOutput).foreach(requireFile)

    val reference = buildReferenceData(rawInput)
    val task11Expected = buildTask11Expected(reference)
    val task12LocalExpected = buildTask12Expected(reference, global = false)
    val task12GlobalExpected = buildTask12Expected(reference, global = true)

    val task11Mismatches = validateTask11(task11Output, task11Expected)
    val task12GlobalMismatches =
      validateTask12(task12GlobalOutput, task12GlobalExpected, expectedScope = "global")
    val task12LocalMismatches =
      validateTask12(task12LocalOutput, task12LocalExpected, expectedScope = "local")

    println("Task 1 independent validation")
    println(s"Input data records: ${reference.inputRecords}")
    println(s"Malformed CSV records: ${reference.malformedRecords}")
    println(s"Valid bought records: ${reference.stateBoughtCounts.values.sum}")
    println(s"Global eligible styles: ${reference.globallyEligibleStyles.size}")
    println(
      s"Task 1-1: expected=${task11Expected.size}, " +
        s"actual=${readCsv(task11Output).size}, mismatches=${task11Mismatches.size}"
    )
    println(
      s"Task 1-2 global: expected=${task12GlobalExpected.size}, " +
        s"actual=${readCsv(task12GlobalOutput).size}, mismatches=${task12GlobalMismatches.size}"
    )
    println(
      s"Task 1-2 local: expected=${task12LocalExpected.size}, " +
        s"actual=${readCsv(task12LocalOutput).size}, mismatches=${task12LocalMismatches.size}"
    )

    val allMismatches =
      task11Mismatches.map("Task 1-1: " + _) ++
        task12GlobalMismatches.map("Task 1-2 global: " + _) ++
        task12LocalMismatches.map("Task 1-2 local: " + _)

    if (allMismatches.nonEmpty) {
      println("First validation mismatches:")
      allMismatches.take(20).foreach(message => println(s"  - $message"))
      throw new IllegalStateException(s"Validation failed with ${allMismatches.size} mismatch(es)")
    }

    println("VALIDATION PASSED: every output key and value matches the local reference calculation.")
  }

  private def requireFile(path: Path): Unit =
    require(Files.isRegularFile(path), s"Required file does not exist: $path")

  /**
   * Đọc input đúng một lần. Parser và normalization dùng chung contract của bài;
   * phần aggregation tham chiếu bên dưới chỉ dùng collection local, không dùng
   * bất kỳ Mapper, Reducer, Writable hay Hadoop shuffle nào.
   */
  private def buildReferenceData(path: Path): ReferenceData = {
    val stateCounts = mutable.HashMap.empty[String, Long].withDefaultValue(0L)
    val bought = mutable.ArrayBuffer.empty[BoughtObservation]
    val styles = mutable.HashMap.empty[(String, String, String), StyleStats]
    val globallyEligible = mutable.HashSet.empty[String]
    var inputRecords = 0L
    var malformedRecords = 0L

    Using.resource(scala.io.Source.fromFile(path.toFile, StandardCharsets.UTF_8.name())) { source =>
      source.getLines().zipWithIndex.foreach { case (line, index) =>
        if (!(index == 0 && AmazonCsv.isHeader(line))) {
          inputRecords += 1L
          AmazonCsv.parse(line) match {
            case Left(_) =>
              malformedRecords += 1L
            case Right(record) =>
              (record.style, record.sizeRank) match {
                case (Some(style), Some(rank)) if rank >= 6 =>
                  globallyEligible += style
                case _ => ()
              }

              if (record.isBought) {
                record.shipState.filter(_ => record.hasValidState).foreach { state =>
                  stateCounts.update(state, stateCounts(state) + 1L)
                  (record.orderDate, record.size) match {
                    case (Some(date), Some(size)) =>
                      bought += BoughtObservation(state, date, size, record.amount)
                    case _ => ()
                  }
                }
              }

              (
                record.shipState.filter(_ => record.hasValidState),
                record.orderMonth,
                record.style,
                record.sku
              ) match {
                case (Some(state), Some(month), Some(style), Some(sku)) =>
                  val stats = styles.getOrElseUpdate((state, month, style), new StyleStats)
                  stats.add(sku, record.sizeRank.getOrElse(0))
                case _ => ()
              }
          }
        }
      }
    }

    ReferenceData(
      inputRecords,
      malformedRecords,
      stateCounts.toMap,
      bought.toVector,
      styles.toMap,
      globallyEligible.toSet
    )
  }

  private def buildTask11Expected(
      reference: ReferenceData
  ): Map[(String, String), WindowExpected] = {
    val buckets = mutable.HashMap.empty[(String, LocalDate, String), Moments]

    reference.boughtObservations.foreach { observation =>
      val windowDays =
        if (reference.stateBoughtCounts(observation.state) > 10000L) 5 else 10
      var offset = 1
      while (offset <= windowDays) {
        val key = (observation.state, observation.date.plusDays(offset.toLong), observation.size)
        buckets.getOrElseUpdate(key, new Moments).add(observation.amount)
        offset += 1
      }
    }

    buckets.toSeq
      .groupBy { case ((state, date, _), _) => (state, date) }
      .map { case ((state, date), candidates) =>
        val winner = candidates.iterator
          .map { case ((_, _, size), moments) =>
            WindowExpected(size, moments.purchaseCount, moments.variance)
          }
          .reduceLeft { (best, candidate) =>
            if (candidateWins(candidate, best)) candidate else best
          }
        (state, date.toString) -> winner
      }
  }

  private def candidateWins(candidate: WindowExpected, best: WindowExpected): Boolean =
    candidate.purchaseCount > best.purchaseCount ||
      (candidate.purchaseCount == best.purchaseCount &&
        compareVariance(candidate.variance, best.variance) < 0) ||
      (candidate.purchaseCount == best.purchaseCount &&
        compareVariance(candidate.variance, best.variance) == 0 &&
        candidate.size.compareTo(best.size) < 0)

  private def compareVariance(left: Option[Double], right: Option[Double]): Int =
    (left, right) match {
      case (Some(a), Some(b)) => java.lang.Double.compare(a, b)
      case (Some(_), None)    => -1
      case (None, Some(_))    => 1
      case (None, None)       => 0
    }

  private def buildTask12Expected(
      reference: ReferenceData,
      global: Boolean
  ): Map[(String, String), MedianExpected] = {
    val varietiesByGroup = mutable.HashMap.empty[(String, String), mutable.ArrayBuffer[Int]]

    reference.styleStats.foreach { case ((state, month, style), stats) =>
      val eligible =
        if (global) reference.globallyEligibleStyles.contains(style)
        else stats.maxSizeRank >= 6
      if (eligible) {
        varietiesByGroup
          .getOrElseUpdate((state, month), mutable.ArrayBuffer.empty)
          .append(stats.skus.size)
      }
    }

    varietiesByGroup.iterator.map { case (key, varieties) =>
      val sorted = varieties.sorted
      val middle = sorted.length / 2
      val median =
        if (sorted.length % 2 == 1) sorted(middle).toDouble
        else (sorted(middle - 1).toDouble + sorted(middle).toDouble) / 2.0
      key -> MedianExpected(median, sorted.length)
    }.toMap
  }

  private def validateTask11(
      path: Path,
      expected: Map[(String, String), WindowExpected]
  ): Vector[String] = {
    val rows = readCsv(path)
    val duplicateKeys = duplicateKeyMessages(rows, row => (row("state"), row("window_date")))
    val actual = rows.map { row =>
      val key = (row("state"), row("window_date"))
      val variance = Option(row("population_variance")).filter(_.nonEmpty).map(_.toDouble)
      key -> WindowExpected(row("size"), row("purchase_count").toLong, variance)
    }.toMap

    duplicateKeys ++ compareKeySets(expected.keySet, actual.keySet) ++
      expected.keysIterator.flatMap { key =>
        actual.get(key).toVector.flatMap { found =>
          val wanted = expected(key)
          val varianceMatches = (wanted.variance, found.variance) match {
            case (Some(a), Some(b)) => math.abs(a - b) <= 1e-6
            case (None, None)       => true
            case _                  => false
          }
          if (
            wanted.size == found.size &&
            wanted.purchaseCount == found.purchaseCount &&
            varianceMatches
          ) Vector.empty
          else Vector(s"$key expected=$wanted actual=$found")
        }
      }.toVector
  }

  private def validateTask12(
      path: Path,
      expected: Map[(String, String), MedianExpected],
      expectedScope: String
  ): Vector[String] = {
    val rows = readCsv(path)
    val duplicateKeys = duplicateKeyMessages(rows, row => (row("state"), row("month")))
    val scopeErrors = rows.collect {
      case row if row("eligibility_scope") != expectedScope =>
        s"Unexpected scope for ${(row("state"), row("month"))}: ${row("eligibility_scope")}"
    }
    val actual = rows.map { row =>
      (row("state"), row("month")) ->
        MedianExpected(row("median_variety").toDouble, row("eligible_style_count").toInt)
    }.toMap

    duplicateKeys ++ scopeErrors ++ compareKeySets(expected.keySet, actual.keySet) ++
      expected.keysIterator.flatMap { key =>
        actual.get(key).toVector.flatMap { found =>
          val wanted = expected(key)
          if (
            math.abs(wanted.median - found.median) <= 1e-9 &&
            wanted.eligibleStyleCount == found.eligibleStyleCount
          ) Vector.empty
          else Vector(s"$key expected=$wanted actual=$found")
        }
      }.toVector
  }

  private def duplicateKeyMessages(
      rows: Vector[Map[String, String]],
      keyOf: Map[String, String] => (String, String)
  ): Vector[String] =
    rows.groupBy(keyOf).collect {
      case (key, duplicates) if duplicates.size > 1 =>
        s"Duplicate output key $key occurs ${duplicates.size} times"
    }.toVector

  private def compareKeySets(
      expected: Set[(String, String)],
      actual: Set[(String, String)]
  ): Vector[String] = {
    val missing = (expected -- actual).toVector.sorted.map(key => s"Missing output key $key")
    val unexpected = (actual -- expected).toVector.sorted.map(key => s"Unexpected output key $key")
    missing ++ unexpected
  }

  private def readCsv(path: Path): Vector[Map[String, String]] = {
    val format = CSVFormat.DEFAULT.builder()
      .setHeader()
      .setSkipHeaderRecord(true)
      .build()
    Using.Manager { use =>
      val reader = use(Files.newBufferedReader(path, StandardCharsets.UTF_8))
      val parser = use(format.parse(reader))
      parser.iterator().asScala.map(record => record.toMap.asScala.toMap).toVector
    }.get
  }
}
