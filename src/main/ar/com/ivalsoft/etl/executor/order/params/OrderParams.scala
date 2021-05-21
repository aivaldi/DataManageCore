package ar.com.ivalsoft.etl.executor.order.params

import ar.com.ivalsoft.etl.executor.params.Params
import ar.com.ivalsoft.etl.executor.params.ColumnClausePair
import ar.com.ivalsoft.etl.executor.params.ColumnOrderPair

/**
 * @author aivaldi
 */
case class OrderParams(filterColumns:Iterable[ColumnOrderPair] ) extends Params {
  
}