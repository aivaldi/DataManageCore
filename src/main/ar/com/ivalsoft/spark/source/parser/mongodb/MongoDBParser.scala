package ar.com.ivalsoft.spark.source.parser.mongodb

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.config._
import ar.com.ivalsoft.spark.source.parser.SourceParser

class MongoDBParser(params: MongoDBParams) extends SourceParser {

  def toDataFrame(sc: SparkSession): Option[DataFrame] = {

    log.info(s"MongoDBParser creating DataFrame")

    var connectionString = "mongodb://";
    if (params.username.nonEmpty)
      connectionString = connectionString + params.username + ":" + params.password + "@"  

    connectionString = connectionString + params.host

    if (params.port.nonEmpty)
      connectionString =  connectionString + ":" + params.port

    connectionString = connectionString + "/" + params.database + "." + params.collection    

    if (params.ssl)
      connectionString = connectionString + "?ssl=true"

    log.info(s"MongoDBParser connection string: $connectionString")

    val readConfig = ReadConfig(Map("uri" -> connectionString))
    val df = sc.read.format("com.mongodb.spark.sql").options(readConfig.asOptions).load()
    
    log.info("MongoDBParser DataFrame created")
    
    Some(df)
  }
} 