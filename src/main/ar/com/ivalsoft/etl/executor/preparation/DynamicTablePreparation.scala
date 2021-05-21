package ar.com.ivalsoft.etl.executor.preparation

import ar.com.ivalsoft.etl.executor.Executor
import scala.concurrent.ExecutionContext
import org.apache.spark.SparkContext
import ar.com.ivalsoft.etl.executor.ExecutorIn
import org.apache.log4j.Logger
import scala.concurrent.Future
import org.apache.spark.sql.DataFrame
import ar.com.ivalsoft.etl.executor.preparation.params.CleaningPreparationParams
import ar.com.ivalsoft.etl.executor.preparation.params.DynamicTableParams
import ar.com.ivalsoft.etl.executor.EtlSqlContext
import org.apache.spark.sql.Row
import ar.com.ivalsoft.etl.executor.params.ColumnAlias
import org.apache.spark.sql.types.StructType
import java.util.Arrays.ArrayList
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructField
import ar.com.ivalsoft.etl.executor.preparation.params.DynamicTableFieldsParams
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._

/**
 * @author aivaldi
 */
class DynamicTablePreparation(implicit sc: SparkSession, ec: ExecutionContext) extends Executor  {


  
    val log = Logger.getLogger(getClass.getName)

  def execute: Future[Option[DataFrame]] = {

    if (currentFuture != null)
      return currentFuture;
    else {
      log.info(s"Preparing Cleaning id:${this.id}")
      
      if (_param == null)
        throw new IllegalArgumentException("No columns selected")

      val param = _param.asInstanceOf[DynamicTableParams]
      if (param.fields== null)
        throw new IllegalArgumentException("No columns selected")

      if (param.fields.size== 0)
        throw new IllegalArgumentException("No columns selected")
      
      if (param.data== null)
        throw new IllegalArgumentException("No data selected")

      if (param.data.size== 0)
        throw new IllegalArgumentException("No data selected")
      
      Future{ 
          Some(
            sc.createDataFrame(param.data.map { r => Row.fromSeq(r.toSeq) }.toList.asJava, getSchema(param.fields) )
            )
        }

      }
    }
  
    /*def getRows(data:Iterable[Iterable[Any]]):RDD[Row] = {
      log.info(s"Generating rows for dynamic data table")
      log.info(s"number of rows: ${data.size}")
      log.info(s"number of columns: ${data.toSeq(0).size}")
            
      
      val ret = data.toSeq.map { 
                r => 
                    Row.fromSeq(r.toSeq) 
              }
      
      sc.createDataFrame( ret)
      
    }*/
    
    def getSchema(data:Iterable[DynamicTableFieldsParams]):StructType = {
    
      StructType(
          data.map { x => x.toStructField() } toSeq   
      )
      
      
    
    }
  
  
}

