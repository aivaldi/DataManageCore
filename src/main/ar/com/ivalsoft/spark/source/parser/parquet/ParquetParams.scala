package ar.com.ivalsoft.spark.source.parser.parquet

import ar.com.ivalsoft.etl.executor.params.Params

/**
 * @author aivaldi
 */

 case class ParquetInParams (path:String) extends Params
 case class ParquetOutParams (path:String, saveMode:String="A") extends Params 

