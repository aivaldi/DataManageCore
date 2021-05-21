package ar.com.ivalsoft.etl.executor.join.params

import ar.com.ivalsoft.etl.executor.params.Params
import ar.com.ivalsoft.etl.executor.params.ColumnClausePair
import ar.com.ivalsoft.etl.executor.params.ColumnAlias


/**
 * @author aivaldi
 */
case class UnionParams(
    colsTableL:Iterable[String],
    colsTableR:Iterable[String],
    newColsTableL:Option[Iterable[String]],
    newColsTableR:Option[Iterable[String]]
    ) extends Params {
  
}