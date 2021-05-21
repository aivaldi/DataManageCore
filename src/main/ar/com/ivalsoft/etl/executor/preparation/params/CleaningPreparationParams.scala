package ar.com.ivalsoft.etl.executor.preparation.params

import ar.com.ivalsoft.etl.executor.params.ColumnAlias
import ar.com.ivalsoft.etl.executor.params.Params

/**
 * @author aivaldi
 */

case class CleaningPreparationParams(formulaList:Iterable[CleaningParam] )  extends Params

case class CleaningParam (columnName:String, formula:String)