package ar.com.ivalsoft.etl.executor

import org.apache.spark.sql.DataFrame
import scala.concurrent.Future
/**
 * @author aivaldi
 */
trait ExecutorIn{
  protected var _dfList:Seq[Future[Option[DataFrame]]]=null
  def dfListIn:Seq[Future[Option[DataFrame]]] = _dfList; 
  def dfListIn_= (value:Seq[Future[Option[DataFrame]]]):Unit = _dfList = value 
}