package ar.com.ivalsoft.etl.executor.writer.parquet

import org.apache.spark.SparkContext
import ar.com.ivalsoft.etl.executor.Executor
import scala.concurrent.Future
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import scala.concurrent.ExecutionContext
import ar.com.ivalsoft.spark.source.parser.sql.SQLParser
import ar.com.ivalsoft.spark.source.parser.sql.SQLReaderParser
import org.apache.log4j.Logger
import ar.com.ivalsoft.etl.executor.ExecutorIn
import ar.com.ivalsoft.spark.source.persister.SourcePersister
import ar.com.ivalsoft.spark.source.persister.sql.SQLPersister
import ar.com.ivalsoft.spark.source.persister.sql.SQLConnection
import ar.com.ivalsoft.spark.source.executor.ExecutionInformation
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import ar.com.ivalsoft.spark.source.persister.parquet.ParquetConnection
import ar.com.ivalsoft.spark.source.parser.parquet.ParquetOutParams
import ar.com.ivalsoft.spark.source.persister.parquet.ParquetPersister

/**
 * @author aivaldi
 */
class ParquetDataWriter(implicit sc: SparkSession, ec: ExecutionContext) extends Executor with ExecutorIn {

  val log = Logger.getLogger(getClass.getName)

  def execute: Future[Option[DataFrame]] = {

    if (dfListIn.length != 1)
      throw new IllegalArgumentException("DataFrame to execute must be equal to 1")

    dfListIn(0).map {
      case Some(df: DataFrame) =>
        {
          log.info(s"ParquetDataWriter (${this.id}) starting execution.")
          val t0 = System.nanoTime()
          var sqlContext = (sc)

          val sourceWriter = createWriter
          val ei = sourceWriter.saveData(Some(df))
          val t1 = System.nanoTime()
          executionInformation = ei
          log.info(s"ParquetDataWriter (${this.id}) finished execution.")
          None;
        }
      case None => None
    }

  }

  def createWriter: SourcePersister = {

    if (param == null) {
      log.error("Parameters for DB access not configured")
      throw new Exception("param is null")
    }
    val sourcePersister = new ParquetPersister(new ParquetConnection(param.asInstanceOf[ParquetOutParams]))

    log.info(s"ParquetDataWriter (${this.id}) folder: ${sourcePersister.parquetConnection.parquetParam.path}")

    sourcePersister
  }

}