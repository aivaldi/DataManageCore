package ar.com.ivalsoft.spark.source.parser.json

import ar.com.ivalsoft.spark.source.parser.SourceParser
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Column

class JsonParser(parameter: JsonParams) extends SourceParser {

  override def toDataFrame(sc: SparkSession): Option[DataFrame] = {
    log.info("JSONParser creando RDD")
    
//    val res = scala.io.Source.fromURL(parameter.uri).mkString
    
    val df = sc.read.json(sc.sparkContext.wholeTextFiles(parameter.uri).values)
    val flatDF = df.select(flattenSchema(df.schema):_*)
    
    log.info("JSONParser RDD Creado")

    Some(flatDF)
  }

  def flattenSchema(schema: StructType, prefix: String = null): Array[Column] = {
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else (prefix + "." + f.name)
      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        case _              => Array(org.apache.spark.sql.functions.col(colName).as(colName))
      }
    })
  }

}
