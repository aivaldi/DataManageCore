package ar.com.ivalsoft.etl.information

case class SourceInformation(
  columns: Iterable[SourceInformationColumns],
  rows: Iterable[Iterable[SourceInformationCell]],
  Info: SourceInformationAnalytic)

case class SourceInformationColumns(
  name: String,
  dataType: String)

case class SourceInformationAnalytic(
  numRows: Long)

/**
 * @author aivaldi
 */
trait SourceInformationExecutor {
  def getInformation: SourceInformation;
}
