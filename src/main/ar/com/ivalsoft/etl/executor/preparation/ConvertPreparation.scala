package ar.com.ivalsoft.etl.executor.preparation

import ar.com.ivalsoft.etl.executor.Executor
import scala.concurrent.ExecutionContext
import org.apache.spark.SparkContext
import ar.com.ivalsoft.etl.executor.ExecutorIn
import org.apache.log4j.Logger
import scala.concurrent.Future
import org.apache.spark.sql.DataFrame
import ar.com.ivalsoft.etl.executor.params.ColumnAlias
import ar.com.ivalsoft.etl.executor.preparation.params.SelectionPreparationParams
import ar.com.ivalsoft.etl.executor.preparation.params.ConvertPreparationParams
import org.apache.spark.sql.Column
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StringType, DecimalType, FloatType, DoubleType, LongType, IntegerType, ShortType, ByteType, DateType, TimestampType}
import org.apache.spark.sql.functions._

/**
 * @author aivaldi
 */
class ConvertPreparation(implicit sc: SparkSession, ec: ExecutionContext) extends Executor with ExecutorIn {

  val log = Logger.getLogger(getClass.getName)

  val typeMap = Map("StringType" -> DataTypes.StringType,
    "BooleanType" -> DataTypes.BooleanType,
    "ByteType" -> DataTypes.ByteType,
    "ShortType" -> DataTypes.ShortType,
    "IntegerType" -> DataTypes.IntegerType,
    "LongType" -> DataTypes.LongType,
    "FloatType" -> DataTypes.FloatType,
    "DoubleType" -> DataTypes.DoubleType,
    "DecimalType(18,2)" -> DataTypes.createDecimalType(18, 2),
    "DecimalType(18,3)" -> DataTypes.createDecimalType(18, 3),
    "DecimalType(18,4)" -> DataTypes.createDecimalType(18, 4),
    "DateType" -> DataTypes.DateType,
    "TimestampType" -> DataTypes.TimestampType) 

  def execute: Future[Option[DataFrame]] = {

    if (currentFuture != null)
      return currentFuture;
    else {
      log.info(s"Convert preparation id:${this.id}")
      if (dfListIn.length != 1)
        throw new IllegalArgumentException("DataFrame to execute must be equal to 1")

      if (_param == null)
        throw new IllegalArgumentException("No columns selected")

      if (_param.asInstanceOf[ConvertPreparationParams].columns == null)
        throw new IllegalArgumentException("No columns selected")

      if (_param.asInstanceOf[ConvertPreparationParams].columns.size == 0)
        throw new IllegalArgumentException("No columns selected")



      dfListIn(0).flatMap {
        case Some(df: DataFrame) =>
          {
            log.info(s"Executing Selection id:${this.id}")
            
            val columnCast= _param.asInstanceOf[ConvertPreparationParams].columns 
              
            
            val colName =  _param.asInstanceOf[ConvertPreparationParams].columns.map { _.columnName } toSeq
              
            val columns = df.columns.map {
              col => 
                val colC = columnCast.find { x => x.columnName==col }
                
                if (colC.isEmpty)
                  df.col(col)
                else
                  if (!colC.get.castAs.isEmpty) {
                    // Converts common non dot base numeric formats
                    val dateFormat = colC.get.dateFormat.getOrElse("yyyy-MM-dd")
                    
                    df.schema(col).dataType match {
                      case StringType => 
                        typeMap(colC.get.castAs.get) match {
                          case _ : DecimalType | FloatType | DoubleType | LongType | IntegerType | ShortType | ByteType => 
                              regexp_replace(df.col(col), ",", ".").cast(typeMap(colC.get.castAs.get)).alias(col)
                          case _ : DateType =>
                              //df.withColumn(col, expr(s"TO_DATE(CAST(UNIX_TIMESTAMP($col, '$dateFormat') AS TIMESTAMP))")).col(col)
                              to_date(unix_timestamp(df.col(col), dateFormat).cast(TimestampType)).alias(col)
                          case _ : TimestampType =>
                              unix_timestamp(df.col(col), dateFormat).cast(TimestampType).alias(col)
                          case _ => df.col(col).cast(typeMap(colC.get.castAs.get))
                        }
                      case _ => df.col(col).cast(typeMap(colC.get.castAs.get))
                    }
                  }
                  else
                    df.col(col)
            } toSeq

            currentFuture = Future { Some(
                
                df.select(columns:_*)
                
            ) }
            currentFuture
          }
        case None =>
          currentFuture = Future { None }
          currentFuture
      }

    }
  }

}