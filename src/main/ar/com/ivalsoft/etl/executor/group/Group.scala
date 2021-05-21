package ar.com.ivalsoft.etl.executor.group

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import ar.com.ivalsoft.etl.executor.Executor
import ar.com.ivalsoft.etl.executor.ExecutorIn
import ar.com.ivalsoft.etl.executor.group.params.GroupParams
import ar.com.ivalsoft.etl.executor.group.params.GroupParams
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession

/**
 * @author aivaldi
 */
class Group (implicit sc: SparkSession, ec:ExecutionContext) extends Executor with ExecutorIn {
  
    val log = Logger.getLogger(getClass.getName)

  
    def execute:Future[Option[DataFrame]] = {
          if (currentFuture!=null)
             currentFuture;  
          else{
           log.debug("Creating Group")
           if (dfListIn.length!=1)
             throw new IllegalArgumentException("DataFrame to execute must be equal to 1")
           
           if (_param==null)
             throw new IllegalArgumentException("No columns selected")
           
           if (_param.asInstanceOf[GroupParams].groupedColumns==null)
             throw new IllegalArgumentException("No columns selected")
           
           
           val df1 = dfListIn(0)
           
           df1.flatMap {   
             case Some(res) => 
               
               log.debug("Executing Join")
                
                val exprs = _param.asInstanceOf[GroupParams].aggregatedColumns.map ( (aggCol =>  mapping(aggCol.aggMethod)( res.col(aggCol.columnName) ).as(aggCol.columnNewName)   )). toSeq
                
                
                currentFuture = Future{ Some(
                      res.groupBy
                        (_param.asInstanceOf[GroupParams].groupedColumns.head,  
                            _param.asInstanceOf[GroupParams].groupedColumns.tail.toSeq : _*  ).
                              agg(exprs.head, exprs.tail : _*)
                    )
                  }
                currentFuture
             case None => currentFuture = Future{None}
                          currentFuture
           }
        }    
          
   }
    /**
     * esta funcion pasa de text a def
     */
    def mapping (functionName:String)= (c: Column) => {

      functionName.toLowerCase() match {
        case "sum" => sum(c)
        case "avg" => avg(c)
        case "count" => count(c)
        case "max" => max(c)
        case "min" => min(c)
        case "countDistinct" => countDistinct(c);
      }
      
    }
    
}