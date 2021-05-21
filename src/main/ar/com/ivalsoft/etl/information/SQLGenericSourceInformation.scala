package ar.com.ivalsoft.etl.information

import org.apache.spark.SparkContext
import ar.com.ivalsoft.etl.executor.reader.SQLDataReader
import scala.concurrent.ExecutionContext
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import org.apache.spark.sql.DataFrame
import scala.concurrent.{ Future }
import org.apache.log4j.Logger
import scala.collection.mutable.MutableList
import org.apache.spark.sql.SparkSession
import ar.com.ivalsoft.spark.source.parser.sql.SQLGenericParams

/**
 * @author aivaldi
 */
class SQLGenericSourceInformation(implicit val sc: SparkSession, implicit val ec: ExecutionContext) extends SourceInformationExecutor {

  val log = Logger.getLogger(getClass.getName)
  var param: SQLGenericParams = null;

  var queryTop: String = ""
  var queryCount: String = ""

  def getInformation: SourceInformation = {

    val topE = new SQLDataReader
    topE.param = new SQLGenericParams(
      param.className,
      param.connectionString,
      param.table,
      Some(this.queryTop))

    val countE = new SQLDataReader
    countE.param = new SQLGenericParams(
      param.className,
      param.connectionString,
      param.table,
      Some(this.queryCount))

    val res = Await.result(
      Future.sequence(Seq(topE.execute, countE.execute)),
      Duration.Inf)

    if (res.isEmpty)
      return null;

    val countGet = res(1).get;
    val topGet = res(0).get;

    new SourceInformation(getColumns(topGet), getFirstRows(topGet), getInfo(countGet))

  }

  /**
   *
   * Obtengo las columnas del source
   */
  def getColumns(df: DataFrame): Iterable[SourceInformationColumns] = {

    df.schema.map { s => SourceInformationColumns(s.name, s.dataType.toString()) }

  }

  /**
   * Obtengo las columnas del source
   */
  def getFirstRows(df: DataFrame): Iterable[Iterable[SourceInformationCell]] = {

    df.head(10).map {

      row =>
        var i = 0
        row.toSeq.map { cell =>
          val r = new SourceInformationCell(
            cell match {
              case null                => "null"
              case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
              case array: Array[_]     => array.mkString("[", ", ", "]")
              case seq: Seq[_]         => seq.mkString("[", ", ", "]")
              case _                   => cell.toString
            }, row.schema.fields(i).name)
          i = i + 1
          r
        }

    }

  }

  /**
   * Obtiene la info del source
   */
  def getInfo(df: DataFrame): SourceInformationAnalytic = {
    new SourceInformationAnalytic(df.collect()(0).getAs[Number](0).longValue)
  }

}