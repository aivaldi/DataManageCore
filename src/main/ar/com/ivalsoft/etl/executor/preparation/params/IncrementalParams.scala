package ar.com.ivalsoft.etl.executor.function.params

import ar.com.ivalsoft.etl.executor.params.ColumnOrderPair


/**
 * @author aivaldi
 */

case class IncrementalPreparationParams( partitionColumns:Iterable[String],
  column:String,columnName:String,orderColumns:Iterable[ColumnOrderPair])  extends WindowParam
