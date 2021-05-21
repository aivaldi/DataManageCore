package ar.com.ivalsoft.etl.information

import org.apache.spark.SparkContext
import ar.com.ivalsoft.etl.executor.reader.SQLDataReader
import scala.concurrent.ExecutionContext
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Try
import org.apache.spark.sql.DataFrame
import scala.concurrent.{ Future }
import org.apache.log4j.Logger
import scala.collection.mutable.MutableList
import org.apache.spark.sql.SparkSession
import ar.com.ivalsoft.spark.source.parser.sql.SQLParams

/**
 * @author aivaldi
 */
class SQLSourceInformation(implicit val sc: SparkSession, implicit val ec: ExecutionContext) extends SourceInformationExecutor {

  val log = Logger.getLogger(getClass.getName)
  var param: SQLParams = null;

  def getInformation: SourceInformation = {

    val topE = new SQLDataReader
    var selectColumns = "*"

    if (!param.columns.isEmpty) {
      selectColumns = param.columns.get.mkString(",")
    }

    var selectTop = ""
    var selectCount = ""
    param.dataBaseType match {
      case "sql_server" =>
        selectTop = s"(select top 10 ${selectColumns} from ${param.tableName} ) as t"
        selectCount = s"(select count(*) as tot from ${param.tableName} ) as t"
      case "my_sql" =>
        selectTop = s"(select ${selectColumns} from ${param.tableName} limit 10 ) as t"
        selectCount = s"(select count(*) as tot from ${param.tableName})  as t"
    }

    if (!param.query.isEmpty) {
      selectTop = param.dataBaseType match {
        case "sql_server" => s"(select top 10 ${selectColumns} from ${param.query.get} ) as t"
        case "my_sql"     => s"(select ${selectColumns} from ${param.query.get} limit 10) as t"
      }

      selectCount = s"(select count(*) as tot from (${param.query.get}) as l ) as t"
    }

    topE.param = new SQLParams(
      param.dataBaseType,
      param.server,
      param.dataBaseName,
      param.tableName,
      param.user,
      param.password,
      param.port,
      param.integratedSecurity,
      Some(selectTop),
      param.columns)

    val countE = new SQLDataReader
    countE.param = new SQLParams(
      param.dataBaseType,
      param.server,
      param.dataBaseName,
      param.tableName,
      param.user,
      param.password,
      param.port,
      param.integratedSecurity,
      Some(selectCount),
      param.columns)

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
    var count = Try(df.collect()(0).getAs[Int](0))
    if (count.isFailure) 
      new SourceInformationAnalytic(df.collect()(0).getAs[Long](0))
    else
      new SourceInformationAnalytic(count.get)
  }

}