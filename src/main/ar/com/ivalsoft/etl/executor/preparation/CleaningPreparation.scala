package ar.com.ivalsoft.etl.executor.preparation

import ar.com.ivalsoft.etl.executor.Executor
import scala.concurrent.ExecutionContext
import org.apache.spark.SparkContext
import ar.com.ivalsoft.etl.executor.ExecutorIn
import org.apache.log4j.Logger
import scala.concurrent.Future
import org.apache.spark.sql.DataFrame
import ar.com.ivalsoft.etl.executor.preparation.params.CleaningPreparationParams
import org.apache.spark.sql.SparkSession

/**
 * @author aivaldi
 */
class CleaningPreparation(implicit sc: SparkSession, ec: ExecutionContext) extends Executor with ExecutorIn {

  val log = Logger.getLogger(getClass.getName)

  def execute: Future[Option[DataFrame]] = {

    if (currentFuture != null)
      return currentFuture;
    else {
      log.info(s"Preparing Cleaning id:${this.id}")
      if (dfListIn.length != 1)
        throw new IllegalArgumentException("DataFrame to execute must be equal to 1")

      if (_param == null)
        throw new IllegalArgumentException("No columns selected")

      if (_param.asInstanceOf[CleaningPreparationParams].formulaList== null)
        throw new IllegalArgumentException("No formula selected")

      if (_param.asInstanceOf[CleaningPreparationParams].formulaList.size== 0)
        throw new IllegalArgumentException("No formula selected")

      dfListIn(0).flatMap {
        case Some(df: DataFrame) =>
          {
            log.info(s"Executing cleaning id:${this.id}")
            import org.apache.spark.sql.functions._          

            Future {
              var dfRet = df
              Some(
                   _param.asInstanceOf[CleaningPreparationParams].formulaList.foldLeft(dfRet)
                  {
                     case (l,r) =>{
                       dfRet = dfRet.withColumn(
                            r.columnName,
                           expr(r.formula)).as(r.columnName)
                      dfRet
                     }
                  }
                          
                  ) 
              }
          }
        case None =>
          Future { None }
          
      }

    }
  }

}