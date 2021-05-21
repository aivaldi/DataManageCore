package ar.com.ivalsoft.etl.executor.params

/**
 * @author aivaldi
 * Esta clase coresponde a un par de columnas que forman parte del Join
 * es Nombre columnaOrigien, nombre columna Join y la operacion   = !=, etc
 */
case class ColumnClausePair (columnOrigin:String, columnJoin:String, operation:String, isValueOrigin:Boolean=false, isValueJoin:Boolean=false, logicalOperator:Option[String]=None)