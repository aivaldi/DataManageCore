package ar.com.ivalsoft.etl.executor.join

import org.apache.spark.SparkContext
import scala.concurrent.ExecutionContext
import ar.com.ivalsoft.etl.executor.Executor
import ar.com.ivalsoft.etl.executor.ExecutorIn
import scala.concurrent.Future
import org.apache.spark.sql.DataFrame
import ar.com.ivalsoft.etl.executor.join.params.JoinParams
import org.apache.spark.sql.Column
import org.apache.log4j.Logger
import ar.com.ivalsoft.etl.executor.params.ColumnClausePair
import ar.com.ivalsoft.etl.executor.join.params.MultipleJoinParams
import ar.com.ivalsoft.etl.executor.params.ColumnAlias
import ar.com.ivalsoft.etl.executor.join.params.TableJoin
import ar.com.ivalsoft.etl.executor.EtlSqlContext
import org.apache.spark.sql.SparkSession

/**
 * @author aivaldi
 */
class MultipleJoin (implicit sc: SparkSession, ec:ExecutionContext) extends Executor with ExecutorIn {
  
    /**
     * Genero una funcion implicita para el column alias que permite obtener el nombre 
     * completo de la columna con el nombre de la tabla
     * tabla.columna
     */
    implicit class columnAliasToSelectColumn (col:ColumnAlias) {
      def tableColumnName ():String={
        s"${col.table.get}.${col.columnName} as ${col.columnAlias}"
      }
    }

    /**
     * Defino este metodo para que cuando haya una tupla (string,string) tenga un metodo que lo apse a 
     * inner join tabla on query del join 
     */
    implicit class TableJoinConversions (col:TableJoin) {
      def toTableJoin ():String={
        s"${col.joinType} ${col.tableName} on ${col.filter}"
      }
    }
  
    val log = Logger.getLogger(getClass.getName)

    def execute:Future[Option[DataFrame]] = {
        if (currentFuture!=null)
           return currentFuture;  
        else{   
         log.info("Creating Join")
         
         if (dfListIn.length<2)
           throw new IllegalArgumentException("DataFrame to execute must be equal or bigger than 2")
         
         
         
         
         if (_param==null)
           throw new IllegalArgumentException("No columns selected")
         
         val param = _param.asInstanceOf[MultipleJoinParams]
         
         if (param.joinTables==null)
           throw new IllegalArgumentException("No tables selected")
         
         
         if (param.columnSelected==null)
           throw new IllegalArgumentException("No columns selected")
         
         if (param.columnSelected.size==0)
           throw new IllegalArgumentException("No columns selected")
         
         
         val w = Future.sequence( dfListIn )
      
         
         w.flatMap {   
            res => 
              log.info("Executing Multiple Join")
              
              val cols = param.columnSelected.tail.foldLeft( param.columnSelected.head.tableColumnName() )
                {
                  (acumCol,col) => acumCol+" , "+col.tableColumnName() 
                }
              
              
              val join = param.joinTables.foldLeft( "" )
                {
                  (acumCol,col) => acumCol+" "+col.toTableJoin() 
                }
              
              val query = s"SELECT ${cols} FROM ${param.tableName} ${join}" 
              log.info(s"Executing query $query")
              println(query)
              //registor las tablas
              val tableNames:Seq[String] = Seq(param.tableName).union(param.joinTables.map { x => x.tableName }toSeq) 
              
              (res zip tableNames).map { case (df,tableName) => df.get.registerTempTable(tableName) }
              
              
              val sqlContext = sc
               
              currentFuture = Future{
                              Some(  sqlContext.sql(query) )
                      }
          
             currentFuture;   
         
         }
    }
          
   }
    
    def createCondition(x:ColumnClausePair, left:DataFrame, right:DataFrame):Column ={
      import org.apache.spark.sql.functions.trim
      
      x.operation match{
      
        case "==" => trim(left.col( x.columnOrigin )).equalTo( trim(right.col(x.columnJoin)))
        case ">" => trim(left.col( x.columnOrigin )).gt(trim(right.col(x.columnJoin)))
        case "<" => trim(left.col( x.columnOrigin )).lt( trim(right.col(x.columnJoin)))
        case ">=" => trim(left.col( x.columnOrigin )).geq(trim(right.col(x.columnJoin)))
        case "<=" => trim(left.col( x.columnOrigin )).leq( trim(right.col(x.columnJoin)))
        case "<>" => trim(left.col( x.columnOrigin )).notEqual(trim(right.col(x.columnJoin)))
        case _ => trim(left.col( x.columnOrigin )).equalTo( trim(right.col(x.columnJoin)))
      }
      
      
    
      
    }
}