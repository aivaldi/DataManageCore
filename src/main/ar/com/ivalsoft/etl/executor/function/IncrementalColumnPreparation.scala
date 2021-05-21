package ar.com.ivalsoft.etl.executor.function

import ar.com.ivalsoft.etl.executor.Executor
import scala.concurrent.ExecutionContext
import org.apache.spark.SparkContext
import ar.com.ivalsoft.etl.executor.ExecutorIn
import org.apache.log4j.Logger
import scala.concurrent.Future
import org.apache.spark.sql.DataFrame
import ar.com.ivalsoft.etl.executor.function.params.IncrementalPreparationParams
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Column
import ar.com.ivalsoft.etl.executor.params.Params
import org.apache.spark.sql.SparkSession

/**
 * @author aivaldi
 */
class IncrementalColumnsPreparation(implicit sc: SparkSession, ec: ExecutionContext) extends WindowFunction {

  
    def executeWindowFunction(df:DataFrame, _param:Params):DataFrame = {
  
    
      log.info(s"Preparing incremental Columns id:${this.id}")
      if (dfListIn.length != 1)
        throw new IllegalArgumentException("DataFrame to execute must be equal to 1")

      if (_param == null)
        throw new IllegalArgumentException("No columns selected")

      if (_param.asInstanceOf[IncrementalPreparationParams].columnName== null)
        throw new IllegalArgumentException("No column name")

      val p = _param.asInstanceOf[IncrementalPreparationParams]
      
      val orderColumnsOrdered = p.orderColumns.map{ col =>
                      
              if (!df.columns.exists { nameCol => nameCol == col.columnName })
                throw new Exception(s"Column ${col.columnName} not exists, options: ${df.columns.mkString(",")}")

              col.order.toUpperCase() match {

                case "DESC" => desc(col.columnName)
                case "ASC"  => asc(col.columnName)
                case _      => throw new Exception(s"Operation ${col.order.toUpperCase()} not valid, options: DESC, ASC")
              }
            }
      
      val vp = Window.partitionBy( 
          p.partitionColumns.map { m=> new Column(m) } toSeq :_* )
          .orderBy(orderColumnsOrdered.toSeq:_*)
       
      val rowNumberCol = ( row_number ).over(vp) 
        
      df.withColumn(p.columnName, rowNumberCol)

    
  }

}