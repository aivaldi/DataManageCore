package ar.com.ivalsoft.spark.source.parser.sql

import ar.com.ivalsoft.etl.executor.params.Params

/**
 * @author aivaldi
 */
case class SQLParams(
  dataBaseType: String,
  server: String,
  dataBaseName: String,
  tableName: String,
  user: String,
  password: String,
  port: String = "0",
  integratedSecurity: Boolean = false,
  query: Option[String] = None,
  columns: Option[Iterable[String]] = None,
  saveMode: String = "A",
  deleteValues: Boolean = false
) extends Params

case class SQLGenericParams(
  className: String,
  connectionString: String,
  table: String,
  query: Option[String] = None
) extends Params