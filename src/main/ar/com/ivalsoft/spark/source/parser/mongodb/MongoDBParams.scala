package ar.com.ivalsoft.spark.source.parser.mongodb

import ar.com.ivalsoft.etl.executor.params.Params

/**
 * @author fmilano
 */
case class MongoDBParams  (
  host: String,
  port: String,
  database: String,
  collection: String,
  username: String = "",
  password: String = "",
  ssl: Boolean = false  
 ) extends Params