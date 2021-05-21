package ar.com.ivalsoft.spark.source.parser.csv

import ar.com.ivalsoft.etl.executor.params.Params

/**
 * @author aivaldi
 */
case class CSVInParams  (
  file:String,
  header:String="true",
  inferSchema:String="false",
  delimiter:String=",",
  quote:String="\"",
  escape:String="\\",
  //charset:String="ISO-8859-1",
  charset:String="utf-8",
  dateFormat:Option[String]=None,
  nullValue:String=""
 ) extends Params

case class CSVOutParams  (
  file:String,
  saveMode:String="A",
  delimiter:String=";",
  nullValue:String="null"
 ) extends Params