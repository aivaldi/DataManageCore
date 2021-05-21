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
import ar.com.ivalsoft.etl.executor.function.params.WindowFunctionParams
import ar.com.ivalsoft.etl.executor.params.Params
import ar.com.ivalsoft.etl.executor.function.params.WindowParam
import org.apache.spark.sql.SparkSession

/**
 * @author aivaldi
 * 
 * Es la clase que permite ejecutar window function standar
 * 
 */
abstract class WindowFunction (implicit sc: SparkSession, ec:ExecutionContext) extends Executor with ExecutorIn {
  
    
    val log = Logger.getLogger(getClass.getName)

    def execute:Future[Option[DataFrame]] = {
    
         if (currentFuture!=null)
             currentFuture;  
          else{
           log.debug("Creating window function")
           if (dfListIn.length!=1)
             throw new IllegalArgumentException("DataFrame to execute must be equal to 1")
           
           if (_param==null)
             throw new IllegalArgumentException("No column function")
           
           if (_param.asInstanceOf[WindowParam].partitionColumns==null)
             throw new IllegalArgumentException("No columns for partition")
           
           
           val df1 = dfListIn(0)
           
           df1.flatMap {  
             case Some(res) => 
                log.debug("Executing window function")
                
                currentFuture=Future{ Some(
                    executeWindowFunction(res, _param) 
                  )
                }
                currentFuture
                
             case None => currentFuture = Future{None}
                          currentFuture
           }
        }    
   
  }
    /**
     * 
     * Es el metodo que ejecuta la window function
     * Must Override
     */
    def executeWindowFunction(df:DataFrame, param:Params):DataFrame
    
}