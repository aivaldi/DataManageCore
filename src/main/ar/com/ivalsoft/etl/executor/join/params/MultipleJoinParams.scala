package ar.com.ivalsoft.etl.executor.join.params

import ar.com.ivalsoft.etl.executor.params.Params
import ar.com.ivalsoft.etl.executor.params.ColumnClausePair
import ar.com.ivalsoft.etl.executor.params.ColumnAlias


case class TableJoin(
    tableName:String,
    joinType:String,
    filter:String
    ) extends Params {
  
}

/**
 * @author aivaldi
 * TableName es la tabla principal
 * joinTables, conjunto de tablas con el join y el query dentro
 * Columnselected: las columnas que se van a mostrar en el resultado .
 */
case class MultipleJoinParams(
    tableName:String,
    joinTables:Iterable[TableJoin], 
    columnSelected:Iterable[ColumnAlias] 
    ) extends Params {
  
}