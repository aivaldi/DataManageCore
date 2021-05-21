package ar.com.ivalsoft.etl.executor.preparation.params

import ar.com.ivalsoft.etl.executor.params.ColumnAlias
import ar.com.ivalsoft.etl.executor.params.Params

/**
 * @author aivaldi
 */

case class UnPivotPreparationParams(columnsAggrupatedName:String, columns:Iterable[String], columnName:String )  extends Params
