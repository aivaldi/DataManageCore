package ar.com.ivalsoft.spark.source.persister

import org.apache.spark.sql.DataFrame
import ar.com.ivalsoft.spark.source.executor.ExecutionInformation
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


/**
 * @author aivaldi
 * Dado un Tipo de parser guarda los datos en una source diferente
 */
trait SourcePersister {
  
  /**
   * Guarda el parser en  un source
   */
  def saveData(dataframe:Option[DataFrame])(implicit sc:SparkSession):ExecutionInformation
  
}