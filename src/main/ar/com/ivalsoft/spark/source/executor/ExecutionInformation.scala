package ar.com.ivalsoft.spark.source.executor

import ar.com.ivalsoft.spark.source.persister.DefaultConnection

/**
 * @author aivaldi
 * 
 * Informacion sobre la ejecucion de los sources
 */
case class ExecutionInformation (

  param:Option[DefaultConnection],
  recordCount:Option[Long],
  result:Option[Iterable[Iterable[String]]],
  columns:Option[Iterable[String]],
  error:Option[String])
