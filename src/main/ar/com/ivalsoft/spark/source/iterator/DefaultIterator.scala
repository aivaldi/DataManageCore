package ar.com.ivalsoft.spark.source.iterator

import org.apache.spark.sql.DataFrame

/**
 * @author aivaldi
 */
trait DefaultIterator {
  
  def iterateDataFrame(dataFrame:Option[DataFrame]):Option[DataFrame]
  
  
}