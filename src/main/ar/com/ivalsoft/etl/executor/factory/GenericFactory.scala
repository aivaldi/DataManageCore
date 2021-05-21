package ar.com.ivalsoft.etl.executor.factory

import ar.com.ivalsoft.etl.workflow.WorkflowNodes
import ar.com.ivalsoft.etl.executor.Executor
import org.apache.spark.SparkContext
import ar.com.ivalsoft.etl.executor.preparation.SelectionPreparation
import ar.com.ivalsoft.etl.executor.preparation.params.SelectionPreparationParams
import ar.com.ivalsoft.etl.executor.join.params.JoinParams
import ar.com.ivalsoft.etl.executor.join.Join
import ar.com.ivalsoft.etl.executor.filter.Filter
import ar.com.ivalsoft.etl.executor.filter.params.FilterParams
import ar.com.ivalsoft.etl.executor.params.Params
import org.apache.spark.sql.SparkSession
import ar.com.ivalsoft.etl.executor.reader.GenericDataReader
import ar.com.ivalsoft.spark.source.parser.SourceParser

/**
 * @author aivaldi
 */
object GenericFactory {

  /**
   * Define un executor de seleccion
   */
  def buildGeneric[T <: Executor, P <: Params](node: WorkflowNodes)(fac: () => T)
      (implicit sc: SparkSession, ec: scala.concurrent.ExecutionContext): Executor = {
    var selector = fac()
    selector.id = node.id;
    selector.param = node.params.asInstanceOf[P]
    selector
  }

  def buildGenericInput[P <: Params, T <: SourceParser](node: WorkflowNodes)(fac: P => T)
      (implicit sc: SparkSession, ec: scala.concurrent.ExecutionContext): Executor =
        new GenericDataReader(node.id, fac(node.params.asInstanceOf[P]))
}
   
  