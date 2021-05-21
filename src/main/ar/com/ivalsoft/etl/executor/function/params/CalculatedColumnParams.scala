package ar.com.ivalsoft.etl.executor.function.params

import ar.com.ivalsoft.etl.executor.params.Params

/**
 * @author aivaldi
 * 
 * Recibe el string de la funcion a evaluar para la columna calculada
 */
case class CalculatedColumnParams (columnName:String, evalFunction:String) extends Params