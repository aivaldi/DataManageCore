package ar.com.ivalsoft.etl.executor.reader

import org.apache.spark.SparkContext
import ar.com.ivalsoft.etl.executor.Executor
import scala.concurrent.Future
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import scala.concurrent.ExecutionContext
import ar.com.ivalsoft.spark.source.parser.sql.SQLParser
import ar.com.ivalsoft.spark.source.parser.sql.SQLReaderParser
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import java.security.Policy.Parameters
import java.util.Properties
import ar.com.ivalsoft.spark.source.parser.sql.SQLGenericParams
import ar.com.ivalsoft.spark.source.parser.sql.SQLParams

/**
 * @author aivaldi
 */
class SQLDataReader(implicit sc: SparkSession, ec: ExecutionContext) extends Executor {

  val log = Logger.getLogger(getClass.getName)

  def execute: Future[Option[DataFrame]] = {
    if (currentFuture == null)
      currentFuture = Future {

        log.info(s"Executing SQLDataReader id:${this.id}")
        val t0 = System.nanoTime()

        val sourceOrigin = createReader
        val df = sourceOrigin.toDataFrame(sc)
        val t1 = System.nanoTime()
        df
      }
    currentFuture;
  }

  def createReader: SQLParser = {

    if (param == null) {
      log.error("Parameters for DB access not configured")
      throw new Exception("param is null")
    }

    val sourceOrigin = getSourceOrigin()

    sourceOrigin
  }

  def getSourceOrigin(): SQLParser = {

    if (param.isInstanceOf[SQLGenericParams]) {

      if (!param.asInstanceOf[SQLGenericParams].className.isEmpty)
        Class.forName(param.asInstanceOf[SQLGenericParams].className)

      log.info("Executing a Generic JDBC reader")

      var sourceOrigin = new SQLParser with SQLReaderParser {

        override def getURL: String = {
          log.info("connectionString: " + param.asInstanceOf[SQLGenericParams].connectionString)
          param.asInstanceOf[SQLGenericParams].connectionString
        }

        override def getTable: String = {
          if (param.asInstanceOf[SQLGenericParams].query.isEmpty) {
            log.info("super.getTable:" + super.getTable)
            super.getTable
          } else {
            log.info("query:" + param.asInstanceOf[SQLGenericParams].query.get)
            param.asInstanceOf[SQLGenericParams].query.get
          }

        }

      }

      sourceOrigin.tableName = param.asInstanceOf[SQLGenericParams].table
      sourceOrigin

    } else {

      log.info("Executing a non generic JDBC reader")

      var sourceOrigin = new SQLParser with SQLReaderParser {

        override def getTable: String = {
          if (!param.asInstanceOf[SQLParams].columns.isEmpty)
            this.cols = param.asInstanceOf[SQLParams].columns.get.toSeq.mkString(",")
          if (param.asInstanceOf[SQLParams].query.isEmpty)
            super.getTable
          else
            param.asInstanceOf[SQLParams].query.get

        }

      }

      sourceOrigin.dataBaseType = param.asInstanceOf[SQLParams].dataBaseType;
      sourceOrigin.server = param.asInstanceOf[SQLParams].server;
      sourceOrigin.dataBaseName = param.asInstanceOf[SQLParams].dataBaseName
      sourceOrigin.user = param.asInstanceOf[SQLParams].user
      sourceOrigin.password = param.asInstanceOf[SQLParams].password
      sourceOrigin.tableName = param.asInstanceOf[SQLParams].tableName
      sourceOrigin.port = param.asInstanceOf[SQLParams].port
      sourceOrigin.integratedSecurity = param.asInstanceOf[SQLParams].integratedSecurity

      log.info("SQLDataReader configuration")
      log.info(s"sourceOrigin ${sourceOrigin.dataBaseType}")
      log.info(s"Server ${sourceOrigin.server}")
      log.info(s"dataBaseName ${sourceOrigin.dataBaseName}")
      log.info(s"tableName ${sourceOrigin.tableName}")
      log.info(s"integrated Security ${sourceOrigin.integratedSecurity}")

      log.info(s"User ${sourceOrigin.user}")
      log.info(s"Query ${sourceOrigin.getTable}")
      sourceOrigin
    }

  }

}