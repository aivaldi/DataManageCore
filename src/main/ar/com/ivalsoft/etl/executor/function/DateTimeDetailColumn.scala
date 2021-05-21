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
import ar.com.ivalsoft.etl.executor.function.params.DateTimeDetailColumnParams
import org.apache.spark.sql.SparkSession

/**
 * @author aivaldi
 */
class DateTimeDetailColumn (implicit sc: SparkSession, ec:ExecutionContext) extends Executor with ExecutorIn {
  
    val log = Logger.getLogger(getClass.getName)

    def execute:Future[Option[DataFrame]] = {
    
         if (currentFuture!=null)
             currentFuture;  
          else{
           log.debug("Creating Data Time detail")
           if (dfListIn.length!=1)
             throw new IllegalArgumentException("DataFrame to execute must be equal to 1")
           
           if (_param==null)
             throw new IllegalArgumentException("No parameters found")
           
           val param =_param.asInstanceOf[DateTimeDetailColumnParams];
           
           if (param.dataFormat==null)
             throw new IllegalArgumentException("No column function")
           
           if (param.dataFormat=="")
             throw new IllegalArgumentException("No column function")
           
           if (param.peroidColumns==null)
             throw new IllegalArgumentException("No period definition")
           
           if (param.peroidColumns.length==0)
             throw new IllegalArgumentException("No period definition")
           
           
           val df1 = dfListIn(0)
           
           df1.flatMap {  
             case Some(res) => 
                log.debug("Executing calculated column")
                
                
                var dfRet=res
                
                (param.columnName zip param.peroidColumns ) .foreach { data => 
                  val expression = s"""timePeriod( ${param.dateColumnName}, '${param.dataFormat}' , '${data._2}' ) """
                
                  log.info(s"expresion executed $expression")
                
                  import org.apache.spark.sql.functions._          
                  
                  dfRet = dfRet.withColumn(data._1, expr(expression) )
                
                }
                
                
                currentFuture=Future{ Some(
                     dfRet
                  )
                }
                currentFuture
                
             case None => currentFuture = Future{None}
                          currentFuture
           }
        }    
          
   }
}