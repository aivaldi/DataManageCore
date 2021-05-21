package ar.com.ivalsoft.etl.information

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import org.apache.spark.sql.DataFrame

import ar.com.ivalsoft.etl.executor.Executor
import ar.com.ivalsoft.etl.executor.params.Params

class GenericSourceInformationExecutor(dataReader: Executor) {

  def getInformation: SourceInformation = {
    val res = Await.result(dataReader.execute, Duration.Inf)
    if (!res.isEmpty) {
      val result = res.get
      new SourceInformation(getColumns(result), getFirstRows(result), getInfo(result))
    } else {
      null
    }
  }

  def getColumns(df: DataFrame): Iterable[SourceInformationColumns] =
    df.schema.map(s => SourceInformationColumns(s.name, s.dataType.toString()))

  def getFirstRows(df: DataFrame): Iterable[Iterable[SourceInformationCell]] =
    df.head(10).map { //TODO: Why the first 10 rows?
      row =>
        var i = -1
        row.toSeq.map { cell => // this relies in the order map processes the sequence.
          i = i + 1
          new SourceInformationCell(
            cell match {
              case null                => "null"
              case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
              case array: Array[_]     => array.mkString("[", ", ", "]")
              case seq: Seq[_]         => seq.mkString("[", ", ", "]")
              case _                   => cell.toString
            },
            row.schema.fields(i).name)
        }
    }

  def getInfo(df: DataFrame): SourceInformationAnalytic =
    new SourceInformationAnalytic(df.count())

}