package ar.com.ivalsoft.etl.executor.reader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import ar.com.ivalsoft.etl.executor.params.Params
import ar.com.ivalsoft.etl.executor.Executor
import ar.com.ivalsoft.spark.source.parser.SourceParser

class GenericDataReader[T <: SourceParser](id: String, parser: T)(implicit val sc: SparkSession, implicit val ec: ExecutionContext) extends Executor {

  def execute: Future[Option[DataFrame]] = {
    if (currentFuture == null)
      currentFuture = Future(parser.toDataFrame(sc))
    currentFuture
  }

}