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
import org.apache.spark.sql.SparkSession

/**
 * @author aivaldi
 */
class CalculatedColumn (implicit sc: SparkSession, ec:ExecutionContext) extends Executor with ExecutorIn {
  
    
    val log = Logger.getLogger(getClass.getName)

    def execute:Future[Option[DataFrame]] = {
    
         if (currentFuture!=null)
             currentFuture;  
          else{
           log.debug("Creating Group")
           if (dfListIn.length!=1)
             throw new IllegalArgumentException("DataFrame to execute must be equal to 1")
           
           if (_param==null)
             throw new IllegalArgumentException("No column function")
           
           if (_param.asInstanceOf[CalculatedColumnParams].evalFunction==null)
             throw new IllegalArgumentException("No column function")
           
           
           val df1 = dfListIn(0)
           
           df1.flatMap {  
             case Some(res) => 
                log.debug("Executing calculated column")
                import org.apache.spark.sql.functions._          
                
                currentFuture=Future{ Some(
                    res.withColumn(_param.asInstanceOf[CalculatedColumnParams].columnName, expr(_param.asInstanceOf[CalculatedColumnParams].evalFunction)) 
                  )
                }
                currentFuture
                
             case None => currentFuture = Future{None}
                          currentFuture
           }
        }    
          
   }
}