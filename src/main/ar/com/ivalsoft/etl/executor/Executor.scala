package ar.com.ivalsoft.etl.executor

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import scala.concurrent.Promise
import scala.concurrent.{ Future, Promise }
import ar.com.ivalsoft.spark.source.executor.ExecutionInformation
import ar.com.ivalsoft.etl.executor.params.Params
import org.apache.spark.sql.SQLContext
import ar.com.ivalsoft.etl.executor.udf.DateTimePeriodUDF
import org.apache.spark.sql.SparkSession

/**
 * @author aivaldi
 * Es la estructura basica de un ejecutador
 * Reicive una futura ejecucion y espera para ejecutar.
 */

object EtlSqlContext {

  var sqlContext: SparkSession = null

  def registerUDF() = {
    DateTimePeriodUDF.register(sqlContext)
  }

}

abstract class Executor(implicit sc: SparkSession) {

  var executionInformation: ExecutionInformation = null

  var currentFuture: Future[Option[DataFrame]] = null

  protected var _param: Params = null
  def param: Params = _param;
  def param_=(value: Params): Unit = _param = value

  def execute: Future[Option[DataFrame]]
  protected var _id: String = ""
  def id: String = _id;
  def id_=(value: String): Unit = _id = value

}