package ar.com.ivalsoft.etl.executor.function.params

import ar.com.ivalsoft.etl.executor.params.Params

/**
 * @author aivaldi
 * 
 * Dado un campo de fecha su formato, se genera que periodo pertenece
 * o que formato se le da
 * 
 * P = periodo y podemos poner cualquier cosa multiplo de 12 menor a 6
 * P1 = todos los meses = Todos los meses sin Año 
 * P2 = bimestres
 * P3 = trimestre
 * P4 = cuatrimestre
 * P6 = semestre
 * AP1.. AP6 = trimestre mas el año ej AP6 = 20161,20162 y es año 2016 semestre 1
 * A = año
 * M = Mes
 * d = dia
 * 
 * se pueden combinar de cualquier forma.
 * AM MA P1M, etc.
 * 
 * estos datos vienen por Seq para tener mas de una columna
 */
case class DateTimeDetailColumnParams (columnName:Seq[String],dateColumnName:String, dataFormat:String, peroidColumns: Seq[String]) extends Params