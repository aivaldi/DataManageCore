package ar.com.ivalsoft.spark.source.parser

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

/**
 * @author aivaldi
 * 
 * Interface simple que define los metodos que tiene que tener un Parse
 * Para transformar una entrada en un RDD de spark
 */
trait SourceParser {
  
  val log = Logger.getLogger(getClass.getName)
  
  /** Este metodo transforma el objeto en un DataFrame */
  def toDataFrame(sc:SparkSession):Option[DataFrame]
  
}