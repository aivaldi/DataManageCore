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
import org.apache.spark.sql.SparkSession

/**
 * @author aivaldi
 */
class SelectionPreparation(implicit sc: SparkSession, ec: ExecutionContext) extends Executor with ExecutorIn {

  val log = Logger.getLogger(getClass.getName)

  def execute: Future[Option[DataFrame]] = {

    if (currentFuture != null)
      return currentFuture;
    else {
      log.info(s"Preparing Selection id:${this.id}")
      if (dfListIn.length != 1)
        throw new IllegalArgumentException("DataFrame to execute must be equal to 1")

      if (_param == null)
        throw new IllegalArgumentException("No columns selected")

      if (_param.asInstanceOf[SelectionPreparationParams].columns == null)
        throw new IllegalArgumentException("No columns selected")

      if (_param.asInstanceOf[SelectionPreparationParams].columns.size == 0)
        throw new IllegalArgumentException("No columns selected")

      dfListIn(0).flatMap {
        case Some(df: DataFrame) =>
          {
            log.info(s"Executing Selection id:${this.id}")
            val columnNames = (_param.asInstanceOf[SelectionPreparationParams].columns.map { x => x.columnName } toSeq)
            val columnAlias = (_param.asInstanceOf[SelectionPreparationParams].columns.map { x => if (x.columnAlias == null) x.columnName else x.columnAlias } toSeq)

            if (_param.asInstanceOf[SelectionPreparationParams].distinct)
              currentFuture = Future { Some(df.select(columnNames.head, columnNames.tail: _*).toDF(columnAlias: _*)) }
            else
              currentFuture = Future { Some(df.select(columnNames.head, columnNames.tail: _*).toDF(columnAlias: _*).distinct()) }
            currentFuture
          }
        case None =>
          currentFuture = Future { None }
          currentFuture
      }

    }
  }

}