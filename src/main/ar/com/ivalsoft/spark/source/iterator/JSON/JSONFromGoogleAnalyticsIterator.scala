package ar.com.ivalsoft.spark.source.iterator.JSON

// Import Spark SQL data types
import org.apache.spark.sql.types.{StructType,StructField,StringType}
import ar.com.ivalsoft.spark.source.iterator.DefaultIterator
import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Column

/**
 * @author aivaldi
 * 
 * Se achata el JSON para que sea un solo
 * 
 */
trait JSONFromGoogleAnalyticsIterator extends DefaultIterator{
  import scala.collection.JavaConversions._
  
  
  /**
   * Clase que devuelvo cuando infiero la estructura
   */
  case class InferedStruct (struct:StructType, queryColumns:Seq[org.apache.spark.sql.Column] ) 
  
    private val log = Logger.getLogger(getClass.getName)
  
    def iterateDataFrame(dataFrame:Option[DataFrame]):Option[DataFrame]
     ={
     
      log .info("Creando parser para google analytics")
      
      log.info("cargando datos de la estructura json, rows")
      
      if (dataFrame.isEmpty)
        return dataFrame
      
      var df = dataFrame.get
      
      log.info("excluyendo las rows para explotarlas")
     
      log.info("Procesando las filas para crear las columnas a partir de casa row ")
      val res=df;
      /*var res=(df.explode("rows","r") { 
        l: WrappedArray[ArrayBuffer[String]] => l.toList}).select("r")
        .map { m => m.getList[Row](0) }
      
      var u = res.map { m => Row.fromSeq(m.toSeq) }
      log.info("Infiriendo esquema")
      
        
      log.info("Creando dataFrame a partir de las row")
      var df1 = df.sqlContext.createDataFrame(u, getSchema(df)  )
    
      var query = getQuery(df1, df)
      * 
      */
      Some(df)
    
    }
  
  
  
  /**
     * Genera el scheme para obtener la tabla con los nombres de los campos correctos
     *
     * 
     * Devolvemos una case class con un atributo de strunct y otro de seq para los distintos casos 
     * 
     * 
     * Hay un bug en spark-csv parece que no puede convertir los String a Integer
     * por eso devuelvo todo a string y despues lo casteo en el select
     *  
     */
    def getQuery(table:DataFrame,tableBase:DataFrame):Seq[org.apache.spark.sql.Column]={
      
      log.info("Infiriendo encabezado datos del JSON")
      
      val j =  tableBase.select("columnHeaders").collect()(0).getList[Row](0)
      
      val schemaString = j.map { m=> Seq(m(0),m(1),m(2))}
      
      log.info(s"Creando query para las columnas, ${j.length} encontradas")
      
      schemaString.map { 
            column =>
              table.col(sanitizeColumn(column(2).asInstanceOf[String])).cast( getDataType(column(1).asInstanceOf[String] ) )
          }
      
    }
  
    
    def sanitizeColumn(colName:String):String = {
      
      val r=".*:(.*)".r
      
      val res = r.findFirstMatchIn(colName)
      if (res.isEmpty)
        return colName
        
      if (res.get.groupCount==0)
        return res.get.group(0)
      
      return res.get.group(1)
    }
  
    /**
     * Genera el scheme para obtener la tabla con los nombres de los campos correctos
     *
     * 
     * Devolvemos una case class con un atributo de strunct y otro de seq para los distintos casos 
     * 
     * 
     * Hay un bug en spark-csv parece que no puede convertir los String a Integer
     * por eso devuelvo todo a string y despues lo casteo en el select
     *  
     */
    def getSchema(df:DataFrame):StructType={
      
      log.info("Infiriendo encabezado datos del JSON")
      
      val j =  df.select("columnHeaders").collect()(0).getList[Row](0)
      
      val schemaString = j.map { m=> Seq(m(0),m(1),m(2))}
      
      j.foreach { println }
      
      
      StructType(
        schemaString.map{ 
              column =>
                StructField(sanitizeColumn(column(2).asInstanceOf[String]) ,StringType, true )
            }
      )
      
    }
  
    /**
     * Dado un strung devuelvo un dataType de Spark
     */
    def getDataType(value:String):org.apache.spark.sql.types.DataType={
      value match{
        case "STRING"=>return StringType
        case "INTEGER"=>return IntegerType
      }
      StringType
    }
}