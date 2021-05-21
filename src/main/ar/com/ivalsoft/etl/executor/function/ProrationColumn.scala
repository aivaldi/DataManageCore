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
import ar.com.ivalsoft.etl.executor.function.params.ProrationFunctionParams
import org.apache.spark.sql.SparkSession

/**
 * @author aivaldi
 */
class ProrationColumn (implicit sc: SparkSession, ec:ExecutionContext) extends WindowFunction {
  
    
      
    /**
     * Window function de acumulador
     */
    def executeWindowFunction(df:DataFrame, param:Params):DataFrame = {
      
         
         log.debug("Proration window function")
         val p = param.asInstanceOf[ProrationFunctionParams]
         
         val reveDesc = Window.partitionBy(  p.partitionColumns.map { m=> new Column(m) } toSeq :_*)
         
         val reveDiff = (new Column( p.column ))/sum( new Column( p.column ) ).over(reveDesc) *(new Column( p.columnProration ))
         
         df.withColumn(p.columnName, reveDiff)
      
    }
    
}