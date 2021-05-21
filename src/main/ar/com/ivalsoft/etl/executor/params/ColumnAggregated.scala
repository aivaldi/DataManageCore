package ar.com.ivalsoft.etl.executor.params

/**
 * @author aivaldi
 * 
 * @param columnName: nombre de la coluna
 * @param columnNewName: Nuevo nombre que va a tener despues de la agregacion
 * @param aggMethod: Metodo de agregacion a aplicarse a la columna
 */
case class ColumnAggregated (columnName:String, columnNewName:String, aggMethod:String)