package ar.com.ivalsoft.etl.executor.preparation

import ar.com.ivalsoft.etl.executor.Executor
import scala.concurrent.ExecutionContext
import org.apache.spark.SparkContext
import ar.com.ivalsoft.etl.executor.ExecutorIn
import org.apache.log4j.Logger
import scala.concurrent.Future
import org.apache.spark.sql.DataFrame
import ar.com.ivalsoft.etl.executor.params.ColumnAlias
import ar.com.ivalsoft.etl.executor.preparation.params.SelectionPreparationParams
import ar.com.ivalsoft.etl.executor.preparation.params.UnPivotPreparationParams
import org.apache.spark.sql.SparkSession

/**
 * @author aivaldi
 */
class UnPivotPreparation(implicit sc: SparkSession, ec: ExecutionContext) extends Executor with ExecutorIn {

  val log = Logger.getLogger(getClass.getName)

  def execute: Future[Option[DataFrame]] = {

    if (currentFuture != null)
      return currentFuture;
    else {
      log.info(s"Preparing UnPivot id:${this.id}")
      if (dfListIn.length != 1)
        throw new IllegalArgumentException("DataFrame to execute must be equal to 1")

      if (_param == null)
        throw new IllegalArgumentException("Configuration needed")

      var param = _param.asInstanceOf[UnPivotPreparationParams]
      
      if (param.columnsAggrupatedName == null)
        throw new IllegalArgumentException("No column aggregated name given")

      if (param.columns.size == 0)
        throw new IllegalArgumentException("No columns agregated selected")

      if (param.columnName == null)
        throw new IllegalArgumentException("No columns name given")

      dfListIn(0).flatMap {
        case Some(df: DataFrame) =>
          {
           import ar.com.ivalsoft.spark.sql.functions._
           Future { Some(df.unPivot(param.columnsAggrupatedName, param.columnName, param.columns.toSeq:_*)) }
          }
        case None =>
          currentFuture = Future { None }
          currentFuture
      }

    }
  }

}