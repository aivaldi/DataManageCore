package ar.com.ivalsoft.spark.source.parser.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.io.Text
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import ar.com.ivalsoft.spark.source.parser.SourceParser

class ParquetParser(parameter: ParquetInParams) extends SourceParser {

  def toDataFrame(sc: SparkSession): Option[DataFrame] = {
    log.info("ParquetParser creating DataFrame")
    
    val df = sc.read.parquet(parameter.path)
    
    log.info("ParquetParser DataFrame created")
    
    Some(df)
  }

}