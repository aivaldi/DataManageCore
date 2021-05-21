package ar.com.ivalsoft.spark.source.persister.csv

import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode
import ar.com.ivalsoft.spark.source.persister.SourcePersister
import ar.com.ivalsoft.spark.source.executor.ExecutionInformation
import ar.com.ivalsoft.etl.executor.EtlSqlContext
import java.sql.DriverManager
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.SparkSession
import ar.com.ivalsoft.spark.source.persister.DataWriter

/**
 * @author aivaldi
 * Persiste los datos en un Parquet
 */
class CSVPersister(val csvConnection: CSVConnection) extends SourcePersister with DataWriter {

  private val log = Logger.getLogger(getClass.getName)

  def saveData(dataframe: Option[DataFrame])(implicit sc: SparkSession): ExecutionInformation = {

    if (dataframe.isEmpty) {
      log.error("No se encontro el data frame para persistir en CSV")
      return ExecutionInformation(None, None, None,None, Some("DATA_FRAME_NOT_FOUND"))
    }

    val df = dataframe.get

    val sqlContext = (sc)

    import sqlContext.implicits._

    val columnAlias = df.columns.map(_.replaceAll("[ ,;{}()\n\t=]", "_")).toSeq

    df.toDF(columnAlias: _*).coalesce(1).write.format("csv")
    .option("header", "true")
    .option("delimiter", csvConnection.csvParam.delimiter)
    .option("nullValue", csvConnection.csvParam.nullValue)
    .mode(toSaveMode(csvConnection.csvParam.saveMode))
    .save(csvConnection.csvParam.file)

    var res = Some(df.head(10).map { x =>
      x.toSeq.map {
        case null => ""
        case y    => y.toString
      }
    } toSeq)

    new ExecutionInformation(Some(csvConnection), Some(df.count()), res, Some(df.columns), None)
  }

}