package ar.com.ivalsoft.etl.executor.join

import org.apache.spark.SparkContext
import scala.concurrent.ExecutionContext
import ar.com.ivalsoft.etl.executor.Executor
import ar.com.ivalsoft.etl.executor.ExecutorIn
import scala.concurrent.Future
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.log4j.Logger
import ar.com.ivalsoft.etl.executor.join.params.UnionParams
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

/**
 * @author aivaldi
 */
class Union (implicit sc: SparkSession, ec:ExecutionContext) extends Executor with ExecutorIn {
  
    val log = Logger.getLogger(getClass.getName)

    def execute:Future[Option[DataFrame]] = {
        if (currentFuture!=null)
           return currentFuture;  
        else{   
         log.info("Creating Union")
         
         if (dfListIn.length!=2)
           throw new IllegalArgumentException("DataFrame to execute must be equal to 2")
         
         if (_param==null)
           throw new IllegalArgumentException("No columns selected")
         
         if (_param.asInstanceOf[UnionParams].colsTableL==null)
           throw new IllegalArgumentException("No columns selected")
         
         if (_param.asInstanceOf[UnionParams].colsTableL.size==0)
           throw new IllegalArgumentException("No columns selected")
         
         if (_param.asInstanceOf[UnionParams].colsTableR==null)
           throw new IllegalArgumentException("No columns selected")
         
         if (_param.asInstanceOf[UnionParams].colsTableR.size==0)
           throw new IllegalArgumentException("No columns selected")
         
         if (_param.asInstanceOf[UnionParams].colsTableR.size!=_param.asInstanceOf[UnionParams].colsTableL.size)
           throw new IllegalArgumentException("Columns of L and R not seame size")
         
         
         val df1 = dfListIn(0)
         val df2 = dfListIn(1)
         
         val w = Future.sequence(Seq(df1, df2) )
      
         
         w.flatMap {   
            res => 
              log.info("Executing Union")
              
              var res1 = res(0).get
              var res2 = res(1).get
              
              res1.printSchema()
              res2.printSchema()
              
              if (!_param.asInstanceOf[UnionParams].newColsTableL.isEmpty)
                _param.asInstanceOf[UnionParams].newColsTableL.get.map { 
                  case x => res1 = res1.withColumn(x, lit(null: String).cast(StringType)) 
                }
              
              if (!_param.asInstanceOf[UnionParams].newColsTableR.isEmpty)
                _param.asInstanceOf[UnionParams].newColsTableR.get.map { 
                  case x => res2 = res2.withColumn(x, lit(null: String).cast(StringType)) 
                }
              
              println("Table1")
              res1.columns.foreach { x => println(x) }
              
              println("Table2")
              res2.columns.foreach { x => println(x) }
              
              currentFuture = Future{
                              Some(
                                  res1.select( _param.asInstanceOf[UnionParams].colsTableL.head,  _param.asInstanceOf[UnionParams].colsTableL.tail.toSeq : _* )
                                      .unionAll(res2.select(_param.asInstanceOf[UnionParams].colsTableR.head,  _param.asInstanceOf[UnionParams].colsTableR.tail.toSeq : _* ) )                  
                              )
                      }
          
             currentFuture;   
         
         }
    }
          
   }
}