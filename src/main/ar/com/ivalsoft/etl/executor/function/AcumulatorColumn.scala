package ar.com.ivalsoft.etl.executor.function

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import ar.com.ivalsoft.etl.executor.Executor
import ar.com.ivalsoft.etl.executor.ExecutorIn
import ar.com.ivalsoft.etl.executor.group.params.GroupParams
import ar.com.ivalsoft.etl.executor.function.params.CalculatedColumnParams
import ar.com.ivalsoft.etl.executor.params.Params
import ar.com.ivalsoft.etl.executor.function.params.WindowFunctionParams
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

/**
 * @author aivaldi
 */
class AccumulatorColumn (implicit sc: SparkSession, ec:ExecutionContext) extends WindowFunction {
  
    
      
    /**
     * Window function de acumulador
     */
    def executeWindowFunction(df:DataFrame, param:Params):DataFrame = {
      
         
         log.debug("Acumulator window function")
         val p = param.asInstanceOf[WindowFunctionParams]
         
         val orderedColumns = p.orderColumns.map { col =>
                      
              if (!df.columns.exists { nameCol => nameCol == col.columnName })
                throw new Exception(s"Column ${col.columnName} not exists, options: ${df.columns.mkString(",")}")

              col.order.toUpperCase() match {

                case "DESC" => desc(col.columnName)
                case "ASC"  => asc(col.columnName)
                case _      => throw new Exception(s"Operation ${col.order.toUpperCase()} not valid, options: DESC, ASC")
              }
            } toSeq
            
         val reveDesc = Window.partitionBy(  p.partitionColumns.map { m=> new Column(m) } toSeq :_*)
                                  .orderBy( orderedColumns:_* )
                                  .rowsBetween(Long.MinValue,0)
         
         val reveDiff = sum( new Column( p.column ) ).over(reveDesc)
         
         df.withColumn(p.columnName, reveDiff)
      
    }
    
}