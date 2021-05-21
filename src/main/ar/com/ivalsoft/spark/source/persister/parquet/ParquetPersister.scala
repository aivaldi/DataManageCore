package ar.com.ivalsoft.spark.source.persister.parquet

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
import ar.com.ivalsoft.spark.sql.functions
import ar.com.ivalsoft.spark.sql.configuration

/**
 * @author aivaldi
 * Persiste los datos en un Parquet
 */
class ParquetPersister(val parquetConnection: ParquetConnection) extends SourcePersister with DataWriter {

  private val log = Logger.getLogger(getClass.getName)

  def saveData(dataframe: Option[DataFrame])(implicit sc: SparkSession): ExecutionInformation = {

    if (dataframe.isEmpty) {
      log.error("No se encontro el data frame para persistir en Parquet")
      return ExecutionInformation(None, None, None,None, Some("DATA_FRAME_NOT_FOUND"))
    }

    val df = dataframe.get

    val sqlContext = (sc)

    import sqlContext.implicits._

    val columnAlias = df.columns.map(_.replaceAll("[ ,;{}()\n\t=]", "_")).toSeq

    var dfOptimised = df.toDF(columnAlias: _*)
    if (configuration.coalesce.nonEmpty)
        dfOptimised = dfOptimised.coalesce(configuration.coalesce.get)
    dfOptimised.write. format("parquet")
      .mode(toSaveMode(parquetConnection.parquetParam.saveMode))
      .save(parquetConnection.parquetParam.path)

    var res = Some(df.head(10).map { x =>
      x.toSeq.map {
        case null => ""
        case y    => y.toString
      }
    } toSeq)

    new ExecutionInformation(Some(parquetConnection), Some(df.count()), res, Some(df.columns), None)
  }

}