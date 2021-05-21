package ar.com.ivalsoft.etl.executor.preparation.params

import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.DateType
import ar.com.ivalsoft.etl.executor.params.Params

/**
 * @author aivaldi
 */
case class DynamicTableParams (fields:Iterable[DynamicTableFieldsParams], data:Iterable[Iterable[Any]]) extends Params

case class DynamicTableFieldsParams (name:String, tType:String, nullable:Boolean)

object DynamicTableFieldsParams {
  
  implicit class implicitDynamicTableFieldsParams (dynamicTypeField:DynamicTableFieldsParams) {
    
    def toStructField():StructField = {
      StructField(dynamicTypeField.name, toSparkType(dynamicTypeField.tType), dynamicTypeField.nullable)      
    }
    private def toSparkType(xType:String):DataType = {
      
      xType.toLowerCase() match {
        
        case "byte" => ByteType
        case "short" =>ShortType 
        case "int" => IntegerType
        case "long" => LongType
        case "float" => FloatType
        case "double" =>  DoubleType
        case "bigdecimal" =>  DecimalType.USER_DEFAULT
        case "string" =>  StringType
        case "boolean" => BooleanType
        case "timestamp" =>  TimestampType
        case "datetype" => DateType
        case _ => StringType
      }
      
    }
  }
  
}