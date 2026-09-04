package lab3.common

import java.io.StringReader

import org.apache.commons.csv.CSVFormat

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/** Canonical representation of one CSV record; nullable fields remain optional. */
/** Dùng để parse CSV tay cho MapReduce, vì Hadoop java/scala thuần không có thư viện đọc csv như spark */
/** Các thành phần chính:
* - SaleRecord: case class đại diện cho một bản ghi bán hàng, với các trường dữ liệu tương ứng với các cột trong CSV.
* - AmazonCsv: object chứa các phương thức để kiểm tra header, parse một dòng CSV thành SaleRecord, và xử lý các trường hợp lỗi khi parse(trả về Either[String, SaleRecord]).
* - parseFields: phương thức riêng để tách các trường từ một dòng CSV, sử dụng thư viện Apache Commons CSV để đảm bảo việc tách trường chính xác, đặc biệt khi có các dấu phẩy trong dữ liệu.
*/
final case class SaleRecord(
    sourceIndex: Option[Long],
    orderId: Option[String],
    orderDate: Option[java.time.LocalDate],
    status: Option[String],
    fulfilment: Option[String],
    salesChannel: Option[String],
    shipServiceLevel: Option[String],
    style: Option[String],
    sku: Option[String],
    category: Option[String],
    size: Option[String],
    sizeRank: Option[Int],
    asin: Option[String],
    courierStatus: Option[String],
    quantity: Option[Int],
    currency: Option[String],
    amount: Option[Double],
    shipCity: Option[String],
    shipState: Option[String],
    shipPostalCode: Option[String],
    shipCountry: Option[String],
    promotionIds: Vector[String],
    duplicatePromotionCount: Int,
    isB2b: Option[Boolean],
    fulfilledBy: Option[String]
) {
  def promotionCount: Int = promotionIds.size
  def orderMonth: Option[String] = orderDate.map(_.format(java.time.format.DateTimeFormatter.ofPattern(LabRules.MonthPattern)))
  def hasValidState: Boolean = LabRules.isValidState(shipState)
  def isShipped: Boolean = LabRules.isShipped(status)
  def isCancelled: Boolean = LabRules.isCancelled(status)
  def isBought: Boolean = LabRules.isBought(status, quantity)
}

object AmazonCsv {
  private val Format = CSVFormat.DEFAULT.builder().setTrim(false).build()

  def isHeader(line: String): Boolean =
    parseFields(line).toOption.exists(fields => fields == LabRules.RawHeaders)

  /**
   * Parses one physical line. The supplied dataset has quoted commas but no quoted
   * newlines, so Hadoop's line input format remains safe for this file. (dùng cách chia dòng mặc định của Hadoop)
   * Limitations: vì parser này chỉ đúng với dataset này không có quoted (\n), nên nếu gặp dataset khác có quoted newlines thì sẽ parse sai. Cần dùng thư viện csv khác để parse.
   */
  def parse(line: String): Either[String, SaleRecord] =
    parseFields(line).flatMap { fields =>
      if (fields.length != LabRules.RawHeaders.length)
        Left(s"Expected ${LabRules.RawHeaders.length} CSV fields but found ${fields.length}")
      else {
        val promotions = LabRules.parsePromotions(fields(20))
        Right(SaleRecord(
          sourceIndex = LabRules.parseLong(fields(0)),
          orderId = LabRules.optionalText(fields(1)),
          orderDate = LabRules.parseDate(fields(2)),
          status = LabRules.normalizedText(fields(3)),
          fulfilment = LabRules.normalizedText(fields(4)),
          salesChannel = LabRules.normalizedText(fields(5)),
          shipServiceLevel = LabRules.normalizedText(fields(6)),
          style = LabRules.optionalText(fields(7)),
          sku = LabRules.optionalText(fields(8)),
          category = LabRules.optionalText(fields(9)),
          size = LabRules.normalizedText(fields(10)),
          sizeRank = LabRules.sizeRank(fields(10)),
          asin = LabRules.optionalText(fields(11)),
          courierStatus = LabRules.normalizedText(fields(12)),
          quantity = LabRules.parseInt(fields(13)),
          currency = LabRules.normalizedText(fields(14)),
          amount = LabRules.parseDouble(fields(15)),
          shipCity = LabRules.normalizedText(fields(16)),
          shipState = LabRules.normalizeState(fields(17)),
          shipPostalCode = LabRules.normalizePostalCode(fields(18)),
          shipCountry = LabRules.normalizedText(fields(19)),
          promotionIds = promotions.ids,
          duplicatePromotionCount = promotions.duplicateCount,
          isB2b = LabRules.parseBoolean(fields(21)),
          fulfilledBy = LabRules.normalizedText(fields(22))
        ))
      }
    }

  /** Splits one CSV line into its decoded fields. */
  private def parseFields(line: String): Either[String, Vector[String]] =
    Try {
      val parser = Format.parse(new StringReader(line)) // CSVParser, sau getRecords() thì  List<CSVRecord>
      try parser.getRecords.asScala.map(_.toList.asScala.toVector).toVector 
      finally parser.close()
    } match {
      case Success(Vector(record)) => Right(record)
      case Success(records)        => Left(s"Expected one CSV record but found ${records.length}")
      case Failure(error)          => Left(s"Malformed CSV: ${error.getMessage}")
    }
}
