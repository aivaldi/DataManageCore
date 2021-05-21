package ar.com.ivalsoft.spark.source.parser.excel

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

class ExcelParser(parameter: ExcelParams) extends SourceParser {

  def toDataFrame(sc: SparkSession): Option[DataFrame] = {
    log.info("ExcelParser creando RDD")

    val df = sc.read
      .format("com.crealytics.spark.excel") /** note this sets the locale to us-english, which means that numbers might be displayed differently then you expect. Change this to the locale of the Excel file **/
      .option("sheetName", parameter.sheetName) // Required
      .option("useHeader", parameter.header) // Required
      .option("treatEmptyValuesAsNulls", parameter.treatEmptyValuesAsNulls) 
      .option("inferSchema", parameter.inferSchema) 
      .option("startColumn", parameter.startColumn) 
      .option("endColumn", parameter.endColumn) 
      .option("timestampFormat", parameter.dateFormat.getOrElse("dd/MM/yyyy")) 
      .load(parameter.file).toDF()

    log.info("ExcelParser DataFrame created")

    Some(df)
  }

}
