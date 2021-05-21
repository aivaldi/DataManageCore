package ar.com.ivalsoft.spark.source.iterator

import org.apache.spark.sql.DataFrame

/**
 * @author aivaldi
 * 
 * Cuando no se itera informacion se usar un EmptyIterator
 * 
 */
trait EmptyIterator extends DefaultIterator{
  
  def iterateDataFrame(dataFrame:Option[DataFrame]):Option[DataFrame] = {
    
    dataFrame
    
  }
  
}