package ar.com.ivalsoft.etl.executor.function

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import ar.com.ivalsoft.etl.executor.Executor
import ar.com.ivalsoft.etl.executor.ExecutorIn
import ar.com.ivalsoft.etl.executor.group.params.GroupParams
import ar.com.ivalsoft.etl.executor.function.params.CalculatedColumnParams
import ar.com.ivalsoft.etl.executor.params.Params
import ar.com.ivalsoft.etl.executor.function.params.WindowFunctionParams
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import ar.com.ivalsoft.etl.executor.function.params.ProrationFunctionParams
import ar.com.ivalsoft.etl.executor.function.helper.FillGapHelper
import org.joda.time.DateTime
import org.apache.spark.sql.SQLContext
import ar.com.ivalsoft.etl.executor.function.params.FillGapDateTimeFunctionParams
import org.apache.spark.sql.Row
import ar.com.ivalsoft.etl.executor.EtlSqlContext
import org.apache.spark.sql.SparkSession

/**
 * @author aivaldi
 */
class FillGapsDateTime (implicit sc: SparkSession, ec:ExecutionContext) extends WindowFunction {
  
    
      
    /**
     * Window function de acumulador
     */
    def executeWindowFunction(df:DataFrame, param:Params):DataFrame = {
      
         
      log.debug("Fill Gap Date Time window function")
      val p = param.asInstanceOf[FillGapDateTimeFunctionParams]
       
      ///Idea es obtener todos los datos de la particion, generar una tabla de fechas faltantes por cada valor
      ///hacer un union
      df.cache()
      
      val splitedArray =  FillGapHelper.splitTable(df, p.partitionColumns.toSeq:_* )
      
      
      val splitedAggregated = splitedArray.map { x => 
            x.show()
            val f = FillGapHelper.appendGaps(
              x,p.columnsToReplicate.union(p.partitionColumns.toSeq), p.column, 
              p.orderColumns,
              p.periodType, p.step, p.dateFrom, p.dateTo,
              p.partitionColumns.toSeq , p.breakColumn, p.breakValue)
             
            f
          } 
      
      //val ddf = splitedAggregated.head
      
     
    
      //splitedAggregated.tail.foldLeft(applyFillGapWindowFunction(df,ddf, p)){ (z,f) => z.unionAll( applyFillGapWindowFunction(df,f,p) )  } 
      //var dfUnited = splitedAggregated.tail.foldLeft(ddf){ (z,f) => z.unionAll( f)   } 
      
       implicit val sqlContext = sc
      
      sqlContext.createDataFrame(sc.sparkContext.union[Row](splitedAggregated.head, splitedAggregated.tail toSeq:_*), df.schema)
      //p.columnsToReplicate.foreach  {idC => dfUnited = dfUnited.drop(idC)}
      //le saco los campos a repetir
      
     //dfUnited = dfUnited.withColumn(p.column, to_date(dfUnited.col(p.column)) )
     //dfUnited= dfUnited.join(df.select(p.columnsToReplicate.head,p.idColumns.toSeq.union(p.columnsToReplicate.tail).distinct:_*),(p.idColumns.map { idC => dfUnited.col(idC).equalTo(df.col(idC)) } toSeq).reduce( (c1,c2) => c1.and(c2)  ))
       
     //p.idColumns.foreach  {idC => dfUnited = dfUnited.drop(df.col(idC))}

     //dfUnited
    }
    
  
    
    /**
     * Copia los valores del anterios si los datos estan en null
     * 
     * Column es la columna a replicar, column date es la columna que tiene la fecha para realizar el orden
    
    def executeWF(ddf:DataFrame, partitionColumns:Iterable[String],column:String, columnDate:String):DataFrame = {
       val vp = Window.partitionBy( partitionColumns.map { m=> new Column(m) } toSeq :_* ).orderBy(columnDate)
       
      val copy = ( sum (when( ddf.col(column).isNull,0 ).otherwise(1) ) ).over(vp) 
         
      val ddf2 = ddf.withColumn("vp", copy)
     
      val filler = Window.partitionBy( ddf2.col("vp") ).orderBy(columnDate)
       
      val copy2 = (first(column) ).over(filler) 
         
      ddf2.withColumn(column, copy2).drop("vp")
      
    }
    */
}