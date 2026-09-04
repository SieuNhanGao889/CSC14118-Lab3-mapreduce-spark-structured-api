package lab3.common

import java.time.LocalDate
import java.time.format.{DateTimeFormatter, ResolverStyle}
import java.util.Locale

import scala.collection.mutable
import scala.util.Try

/** Shared data contract and business rules from the assignment. */
object LabRules {
  val RawHeaders: Vector[String] = Vector(
    "index", "Order ID", "Date", "Status", "Fulfilment", "Sales Channel ",
    "ship-service-level", "Style", "SKU", "Category", "Size", "ASIN",
    "Courier Status", "Qty", "currency", "Amount", "ship-city", "ship-state",
    "ship-postal-code", "ship-country", "promotion-ids", "B2B", "fulfilled-by",
    "Unnamed: 22"
  )

  val DatePattern: String = "MM-dd-yy"
  val MonthPattern: String = "yyyy-MM"
  val LargeStateOrderThreshold: Long = 10000L
  val ShortWindowDays: Int = 5
  val LongWindowDays: Int = 10
  val MinPromotionActiveDays: Long = 2L
  val MinValidPromotions: Int = 3
  val MinLargeSizeRank: Int = 6
  val ApproxPercentileAccuracy: Int = 10000

  val SizeRanks: Map[String, Int] = Map(
    "XS" -> 1,
    "S" -> 2,
    "M" -> 3,
    "L" -> 4,
    "XL" -> 5,
    "XXL" -> 6,
    "3XL" -> 7,
    "4XL" -> 8,
    "5XL" -> 9,
    "6XL" -> 10
  )

  val StateAliases: Map[String, String] = Map(
    "AR" -> "ARUNACHAL PRADESH",
    "NL" -> "NAGALAND",
    "PB" -> "PUNJAB",
    "PUNJAB/MOHALI/ZIRAKPUR" -> "PUNJAB",
    "RJ" -> "RAJASTHAN",
    "RAJSTHAN" -> "RAJASTHAN",
    "RAJSHTHAN" -> "RAJASTHAN",
    "ORISSA" -> "ODISHA",
    "PONDICHERRY" -> "PUDUCHERRY",
    "NEW DELHI" -> "DELHI"
  )

  private val StrictDateFormatter = DateTimeFormatter
    .ofPattern("MM-dd-uu", Locale.ROOT)
    .withResolverStyle(ResolverStyle.STRICT)

  def optionalText(value: String): Option[String] =
    Option(value).map(_.trim).filter(_.nonEmpty)

  def normalizedText(value: String): Option[String] =
    optionalText(value).map(_.toUpperCase(Locale.ROOT))

  def normalizeState(value: String): Option[String] =
    normalizedText(value)

  /** Optional sensitivity normalization; the graded baseline uses only UPPER/TRIM. */
  def canonicalizeState(value: String): Option[String] =
    normalizeState(value).map(state => StateAliases.getOrElse(state, state))

  def isValidState(state: Option[String]): Boolean =
    state.exists(_ != "APO")

  def normalizePostalCode(value: String): Option[String] =
    optionalText(value).map(_.replaceFirst("[.]0$", ""))

  def sizeRank(value: String): Option[Int] =
    normalizedText(value).flatMap(SizeRanks.get)

  def parseDate(value: String): Option[LocalDate] =
    optionalText(value).flatMap(text => Try(LocalDate.parse(text, StrictDateFormatter)).toOption)

  def parseLong(value: String): Option[Long] =
    optionalText(value).filter(_.matches("[+-]?[0-9]+")).flatMap(text => Try(text.toLong).toOption)

  def parseInt(value: String): Option[Int] =
    optionalText(value).filter(_.matches("[+-]?[0-9]+")).flatMap(text => Try(text.toInt).toOption)

  def parseDouble(value: String): Option[Double] =
    optionalText(value)
      .filter(_.matches("[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)"))
      .flatMap(text => Try(text.toDouble).toOption)

  def parseBoolean(value: String): Option[Boolean] =
    normalizedText(value).collect {
      case "TRUE"  => true
      case "FALSE" => false
    }

  def parsePromotions(value: String): ParsedPromotions = {
    val seen = mutable.LinkedHashSet.empty[String]
    var duplicateCount = 0
    optionalText(value).toSeq
      .flatMap(_.split(",", -1))
      .flatMap(optionalText)
      .foreach { promotionId =>
        if (!seen.add(promotionId)) duplicateCount += 1
      }
    ParsedPromotions(seen.toVector, duplicateCount)
  }

  def isShipped(status: Option[String]): Boolean = status.exists(_.contains("SHIPPED"))
  def isCancelled(status: Option[String]): Boolean = status.exists(_.contains("CANCELLED"))
  def isBought(status: Option[String], quantity: Option[Int]): Boolean =
    isShipped(status) && quantity.exists(_ > 0)

  def windowDays(totalBoughtInState: Long): Int =
    if (totalBoughtInState > LargeStateOrderThreshold) ShortWindowDays else LongWindowDays
}

final case class ParsedPromotions(ids: Vector[String], duplicateCount: Int)
