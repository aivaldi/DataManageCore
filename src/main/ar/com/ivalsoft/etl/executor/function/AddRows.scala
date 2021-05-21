package ar.com.ivalsoft.etl.executor.function

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import ar.com.ivalsoft.etl.executor.Executor
import ar.com.ivalsoft.etl.executor.ExecutorIn
import ar.com.ivalsoft.etl.executor.group.params.GroupParams
import ar.com.ivalsoft.etl.executor.function.params.AddRowParams
import ar.com.ivalsoft.etl.executor.EtlSqlContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types._
import java.sql.Date
import java.sql.Timestamp
import org.apache.spark.sql.SparkSession


/**
 * Con esta clase dado una lista de strings mapeo a los datos del schema que correspondan
 */
class SparkDataTypeStringMapperClass(val row:Iterable[Any]){
  def toSparkDataType (schema:StructType) = {
    (row.zipWithIndex.map { case (x,pos) => 
      if (pos<schema.fields.length) 
        Some(mapSparkDataTypesFromString(x.toString, schema.fields(pos)))
      else
        None
        } toSeq ).flatten
  }
  
  
  private def mapSparkDataTypesFromString( value:String, field:StructField ):Any={
    
    val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
    
    
    field.dataType.typeName match {
      case FIXED_DECIMAL(p,s) => return Decimal (value)
      case _ =>
    }
    
      
    field.dataType match {
      case DateType=> Date.valueOf(value)
      case TimestampType=> Timestamp.valueOf(value)
      /*case BinaryType=>*/ 
      case IntegerType=> value.toInt
      case BooleanType=> value.toBoolean
      case LongType=> value.toLong
      case DoubleType=> value.toDouble
      case FloatType=> value.toFloat
      case ShortType=> value.toShort
      case ByteType=> value.toByte
      case _=> value
      /*case CalendarIntervalType => Decimal(value)*/
    }
        
  }
  
}

/**
 * @author aivaldi
 */
class AddRows (implicit sc: SparkSession, ec:ExecutionContext) extends Executor with ExecutorIn {
  
    implicit def mapSparkType(row:Iterable[Any]) = new SparkDataTypeStringMapperClass(row)
    
    val log = Logger.getLogger(getClass.getName)

    def execute:Future[Option[DataFrame]] = {
    
         if (currentFuture!=null)
             currentFuture;  
          else{
           log.debug("Adding Row")
           if (dfListIn.length!=1)
             throw new IllegalArgumentException("DataFrame to execute must be equal to 1")
           
           if (_param==null)
             throw new IllegalArgumentException("No parameters")
           
           if (_param.asInstanceOf[AddRowParams].columnValues.size==0)
             throw new IllegalArgumentException("No column values")
           
           
           val df1 = dfListIn(0)
           
           df1.flatMap {  
             case Some(res) => 
                log.debug("Executing Adding Rows")
                log.debug(s"Rows to add ${_param.asInstanceOf[AddRowParams].columnValues.size} ")
                log.debug("Size columns" + _param.asInstanceOf[AddRowParams].columnValues.size)
                log.debug("Size columns" + res.schema.fields.length)
                if (_param.asInstanceOf[AddRowParams].columnValues.size>res.schema.fields.length)
                  throw new Exception(s"The number of columns given exceeds size of table. Table cant. columns ${res.schema.fields.length} given ${_param.asInstanceOf[AddRowParams].columnValues.size}")
                
                val rowsRdd = _param.asInstanceOf[AddRowParams].columnValues.map
                  {                  
                      row =>  Row.fromSeq( row.toSparkDataType(res.schema) )
                  } 
                
                currentFuture=Future{ 
                  res.schema.printTreeString()
                  val newData = sc.createDataFrame(
                      sc.sparkContext.parallelize(rowsRdd.toSeq,2), 
                      res.schema)
                  
                  Some(res.unionAll(newData))
                  
                }
                currentFuture
                
             case None => currentFuture = Future{None}
                          currentFuture
           }
        }    
          
   }
}