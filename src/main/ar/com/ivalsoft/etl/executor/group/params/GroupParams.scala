package ar.com.ivalsoft.etl.executor.group.params

import ar.com.ivalsoft.etl.executor.params.Params
import ar.com.ivalsoft.etl.executor.params.ColumnClausePair
import ar.com.ivalsoft.etl.executor.params.ColumnAggregated

/**
 * @author aivaldi
 */
case class GroupParams(groupedColumns:Iterable[String], 
    aggregatedColumns:Iterable[ColumnAggregated] ) extends Params {
  
}