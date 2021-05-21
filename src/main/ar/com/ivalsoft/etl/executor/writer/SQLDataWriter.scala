package ar.com.ivalsoft.etl.executor.writer

import ar.com.ivalsoft.etl.executor.Executor
import scala.concurrent.Future
import org.apache.spark.sql.DataFrame
import scala.concurrent.ExecutionContext
import org.apache.log4j.Logger
import ar.com.ivalsoft.etl.executor.ExecutorIn
import ar.com.ivalsoft.spark.source.persister.SourcePersister
import ar.com.ivalsoft.spark.source.persister.sql.SQLPersister
import ar.com.ivalsoft.spark.source.persister.sql.SQLConnection
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import ar.com.ivalsoft.spark.source.parser.sql.SQLParams

/**
 * @author aivaldi
 */
class SQLDataWriter(implicit sc: SparkSession, ec: ExecutionContext) extends Executor with ExecutorIn {

  val log = Logger.getLogger(getClass.getName)

  def execute: Future[Option[DataFrame]] = {

    if (dfListIn.length != 1)
      throw new IllegalArgumentException("DataFrame to execute must be equal to 1")

    dfListIn(0).map {
      case Some(df: DataFrame) =>
        {
          log.info(s"Executing SQLDataWriter id:${this.id}")
          val t0 = System.nanoTime()
          var sqlContext = (sc)

          val sourceWriter = createWriter
          val ei = sourceWriter.saveData(Some(df))
          val t1 = System.nanoTime()
          executionInformation = ei
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

    val sql = new SQLConnection(param.asInstanceOf[SQLParams])

    val sourcePersister = new SQLPersister(sql)

    log.info("SQLDataWriter configuration")
    log.info(s"DataBase Type ${sql.param.dataBaseType}")
    log.info(s"Server ${sql.param.server}")
    log.info(s"dataBaseName ${sql.param.dataBaseName}")
    log.info(s"tableName ${sql.param.tableName}")
    log.info(s"User ${sql.param.user}")

    sourcePersister
  }

}