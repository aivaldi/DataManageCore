package ar.com.ivalsoft.etl.executor.filter

import org.apache.spark.SparkContext
import scala.concurrent.ExecutionContext
import ar.com.ivalsoft.etl.executor.Executor
import ar.com.ivalsoft.etl.executor.ExecutorIn
import scala.concurrent.Future
import org.apache.spark.sql.DataFrame
import ar.com.ivalsoft.etl.executor.join.params.JoinParams
import org.apache.spark.sql.Column
import ar.com.ivalsoft.etl.executor.filter.params.FilterParams
import org.apache.log4j.Logger
import ar.com.ivalsoft.etl.executor.params.ColumnClausePair
import org.apache.commons.lang.text.StrBuilder
import ar.com.ivalsoft.etl.executor.filter.params.FilterParams
import org.apache.spark.sql.SparkSession

/**
 * @author aivaldi
 */
class Filter(implicit sc: SparkSession, ec: ExecutionContext) extends Executor with ExecutorIn {

  val log = Logger.getLogger(getClass.getName)

  def execute: Future[Option[DataFrame]] = {
    if (currentFuture != null)
      currentFuture;
    else {
      log.debug("Creating Filter")

      if (dfListIn.length != 1)
        throw new IllegalArgumentException("DataFrame to execute must be equal to 1")

      if (_param == null)
        throw new IllegalArgumentException("No columns selected")

      if (_param.asInstanceOf[FilterParams].filterColumns == null)
        throw new IllegalArgumentException("No columns selected")

      val df1 = dfListIn(0)

      df1.flatMap { 
        case Some(res) =>
          log.debug("Executing filter")
          //val resDF = res.where(getCondition(res, _param.asInstanceOf[FilterParams].filterColumns))
          val resDF = res.where(_param.asInstanceOf[FilterParams].filterColumns)
          currentFuture = Future{ Some(resDF) }
          currentFuture
        case None => currentFuture = Future{ None }
                     currentFuture; 
      }
    }
  }

  /**
   * Devuelvo un String de las condiciones
   */
  def getCondition(df: DataFrame, filteredColumns: Iterable[ColumnClausePair]): String = {

    val strBuild = StringBuilder.newBuilder
    var levelParenthesis: Int = 0

    //Itero todas las columnas posibles
    //si es valor, los datos tiene que llegar tal cual se va a poner 'para texto' 12312 para numero, etc.
    //La operacion es el simbolo tal cual exceptio el in que le tengo que poner los datos entre medio
    filteredColumns.foreach { col =>
      //valido que la columna exista
      if (col.columnOrigin != null && !col.isValueOrigin && !df.columns.exists { dfCol => dfCol == col.columnOrigin })
        throw new Exception(s"Column ${col.columnOrigin} not found, options: ${df.columns.mkString(",")}")
      //valido la otra columna destino
      if (col.columnJoin != null && !col.isValueJoin && !df.columns.exists { dfCol => dfCol == col.columnJoin })
        throw new Exception(s"Column ${col.columnJoin} not found, options: ${df.columns.mkString(",")}")

      //dependiendo del operador decido que hacer
      col.operation match {

        //abre parentesis
        case "(" =>
          //agrego al stack que estoy en un nivel mas
          levelParenthesis += 1
          strBuild.append(" ( ")

        //cierra parentesis, puede llegar a concatenar otro valor
        case ")" =>
          levelParenthesis -= 1
          strBuild.append(" ) ")
          if (!col.logicalOperator.isEmpty)
            strBuild.append(s" ${col.logicalOperator.get} ")

        case "=" | ">" | ">=" | "<" | "<=" | "<>" =>
          strBuild.append(s" ${col.columnOrigin}  ${col.operation} ${col.columnJoin} ")
          if (!col.logicalOperator.isEmpty)
            strBuild.append(s" ${col.logicalOperator.get} ")

        case "in" =>
          strBuild.append(s" ${col.columnOrigin}  ${col.operation} ( ${col.columnJoin} ) ")
          if (!col.logicalOperator.isEmpty)
            strBuild.append(s" ${col.logicalOperator.get} ")
      }

    }

    if (levelParenthesis != 0)
      throw new Exception(s"Malformed query filter :${strBuild.toString()}")

    strBuild.toString()

  }
}