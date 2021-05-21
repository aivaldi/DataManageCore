package ar.com.ivalsoft.spark.source.parser.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.log4j.Logger
import ar.com.ivalsoft.spark.source.parser.SourceParser
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

trait SQLParser extends SQLReaderParser {

  def toDataFrame(sc: SparkSession): Option[DataFrame] = {
    log.info("SQL Parser creando RDD")

    val df = this.getDataAccess(sc)
    
    log.info("SQL Parser RDD Creado")
    
    Some(df)
  }
  
  def getDataAccess(sc: SparkSession): DataFrame = {
    val sqlContext = sc

    import sqlContext.implicits._

    log.info("getDataAccess -- URL: " + this.getURL + " -- Table: " + getTable)

    val df = sqlContext.read.format("jdbc").options(Map("url" -> this.getURL,
      "dbtable" -> getTable))
      .load().toDF()

    df
  }

}
