package ar.com.ivalsoft.etl.executor.function.params

import ar.com.ivalsoft.etl.executor.params.Params
import org.joda.time.DateTime
import ar.com.ivalsoft.etl.executor.params.ColumnOrderPair

/**
 * @author aivaldi
 * 
 * Recibe el string de la funcion a evaluar para la columna calculada
 */

trait WindowParam extends Params{
  def partitionColumns:Iterable[String]
  def column:String
  def columnName:String
}
case class WindowFunctionParams (partitionColumns:Iterable[String], orderColumns:Iterable[ColumnOrderPair], column:String,columnName:String) extends WindowParam

case class ProrationFunctionParams (partitionColumns:Iterable[String], column:String,columnProration:String ,columnName:String) extends WindowParam

case class FillGapDateTimeFunctionParams (partitionColumns:Iterable[String], 
    column:String, columnsToReplicate:Seq[String],orderColumns:Iterable[ColumnOrderPair],
    periodType:String, step:Int, 
    dateFrom:DateTime, dateTo:DateTime,
    columnName:String,
    breakColumn:Option[String], breakValue:Option[Seq[String]]) extends WindowParam