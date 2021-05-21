package ar.com.ivalsoft.spark.source.parser.excel

import ar.com.ivalsoft.etl.executor.params.Params

case class ExcelParams(
	file:String,
	sheetName:String="Hoja1",
	header:String="true",
	inferSchema:String="false",
	treatEmptyValuesAsNulls:String="true",
	startColumn:Int=0,
	endColumn:Int=99,
	dateFormat:Option[String]=None
) extends Params