package ar.com.ivalsoft.spark.sql

import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession

/**
 * @author aivaldi
 */
object functions {
  
  
  implicit class VisionarisDataFrameFunctions(val dff:DataFrame){
        def unPivot (groupedColumnsTransposedColumnName:String, columnName:String, colsToTranspose:String*)
        (implicit sc:SparkSession):DataFrame={
          val ret = ArrayBuffer.empty[Row]
          dff.collect().toSeq.foreach 
          { 
              row => 
                //busco las columnas que no quiero trasponer
                val rowValues = dff.columns.filterNot { x => colsToTranspose.contains(x) }.map 
                {
                  cols => row.get(  row.fieldIndex(cols) ) 
                }.toSeq
                // ahora por cada columna a trasnsponer agrego un nuevo registro
                colsToTranspose.foreach 
                  { 
                    colToT =>
                      val value=row.get(row.fieldIndex(colToT) ) 
                      //Nombre de la columna como fila
                      //Valor de la columna esa
                      ret+= Row.fromSeq( rowValues.union(Seq(colToT,value)))
                  }
                
          }
          
         val schemaFields = dff.schema.fields
         val schemaNewFiled = ArrayBuffer.empty[StructField]
         var newStructField:StructField=null
         for (i<-0 until schemaFields.length){
           //esta asignacion la hago siempre total siempre debe ser el mismo tipo de dato el de la nueva columna
           if ( colsToTranspose.contains( schemaFields(i).name ) )
             newStructField = schemaFields(i)
           else
             schemaNewFiled+=schemaFields(i)
         }
         
         //Agrego la columna que contiene los valores de las columnas transpuestas
         schemaNewFiled += StructField(groupedColumnsTransposedColumnName, StringType, true)
         schemaNewFiled += StructField(columnName, newStructField.dataType, newStructField.nullable, newStructField.metadata)
         sc.createDataFrame( sc.sparkContext.parallelize(ret, 1), StructType(schemaNewFiled)  )
          
        }
      
      }
  
}