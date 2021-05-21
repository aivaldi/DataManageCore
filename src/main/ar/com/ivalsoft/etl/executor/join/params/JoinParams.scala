package ar.com.ivalsoft.etl.executor.join.params

import ar.com.ivalsoft.etl.executor.params.Params
import ar.com.ivalsoft.etl.executor.params.ColumnClausePair
import ar.com.ivalsoft.etl.executor.params.ColumnAlias

/**
 * @author aivaldi
 */
case class JoinParams(joinType:String, joinColumns:Iterable[ColumnClausePair], columnSelected:Iterable[ColumnAlias] ) extends Params {
  
}