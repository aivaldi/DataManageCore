package ar.com.ivalsoft.etl.executor.function.params

import ar.com.ivalsoft.etl.executor.params.Params

/**
 * @author aivaldi
 * 
 * @param columnValues : Listado del valor de las columnas en string, despues segun el schema se debe castear al valor correspondiente
 */
case class AddRowParams (columnValues:Iterable[Iterable[Any]]) extends Params