package ar.com.ivalsoft.etl.executor.params

/**
 * @author aivaldi
 */
case class ColumnAlias (columnName:String, columnAlias:String,table:Option[String]=None, castAs:Option[String]=None, dateFormat:Option[String]=None) {
  
}