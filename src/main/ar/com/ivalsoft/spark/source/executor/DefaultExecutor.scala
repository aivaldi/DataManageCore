package ar.com.ivalsoft.spark.source.executor

import ar.com.ivalsoft.spark.source.parser.SourceParser
import ar.com.ivalsoft.spark.source.persister.SourcePersister
import ar.com.ivalsoft.spark.source.iterator.DefaultIterator
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
 * @author aivaldi
 */
trait DefaultExecutor {
  
  implicit val sc:SparkSession
  
  val sourceOrigin:SourceParser
  
  val sourcePersister:SourcePersister
  
  val sourceIterator:DefaultIterator
  
  /**
   * Ejecuta los metodos para procesar los datos
   */
  def execute:ExecutionInformation = {
        
    sourcePersister.saveData(
        sourceIterator.iterateDataFrame(
            sourceOrigin.toDataFrame(sc)))
  
    
  }
  
}