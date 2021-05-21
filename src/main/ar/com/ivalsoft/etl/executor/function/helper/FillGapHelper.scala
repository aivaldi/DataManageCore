package ar.com.ivalsoft.etl.executor.function.helper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.log4j.Logger
import org.joda.time.Period
import org.joda.time.DateTime
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import ar.com.ivalsoft.etl.executor.EtlSqlContext
import org.apache.spark.sql.DataFrame
import java.sql.Timestamp
import java.sql.Date
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.TimestampType
import ar.com.ivalsoft.etl.executor.params.ColumnOrderPair
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author aivaldi
 */
object FillGapHelper {
  
  
  val log = Logger.getLogger(getClass.getName)
    
  def splitTable ( df:DataFrame,  columns:String* ):Seq[DataFrame] = {
    log.info("Spliting dataframe with columns " + columns.toSeq.toString())
    val values = df.select(columns.head, columns.tail:_*).distinct().collect()
    var seqRet = scala.collection.mutable.ListBuffer.empty[DataFrame]
    var col:Column = null;
    values.foreach { 
      x => 
         val filter = 
           columns.foldLeft(col) 
             { 
               (col, columnName) =>  
                 if (col==null)
                   df.col(columnName).equalTo( x.get(x.fieldIndex(columnName))  )
                 else
                   col.and(df.col(columnName).equalTo( x.get(x.fieldIndex(columnName))  ))
           }
         
         seqRet.append( df.filter(  filter ) ) 
         
    }
    
    
    seqRet.toSeq
    
  }
  /**
   * Agrega los gaps faltantes por el campo que se le diga
   * df:DataFrame a procesar
   * columnSerie, la columna de tiempo que hace a la serie va a tener que ser, si no es TimeStamp, devuelve null siempre 
   *  AAAAMMDD hh:mm:ss truncandose en el nivel que corresponda, ej si se pide A entonces os primeros 4 digitos y asi
   * 
   * periodType: puede ser A:Anio, M:mes, D:dia, h:hora, m:minutos, s:segundos
   * step: el incremental de la seria, ej si periodType es D entonces step 1, serian todos los dias, setp 2 seria cada 2 dias
   * SplitColumns es las columnas con las que se hace el split de las tablas, esos valores los saca del df
   */
  def appendGaps(df:DataFrame,replicateColumns:Iterable[String], columnSerie:String, orderColumns:Iterable[ColumnOrderPair],
      periodType:String, step:Int, 
      dateFrom:DateTime, dateTo:DateTime,splitColumns: Seq[String], 
      breakColumn:Option[String], breakValue:Option[Seq[String]] )
    (implicit sc:SparkSession):RDD[Row]= {
    
    implicit val sqlContext = sc.sqlContext
    
    log.info(s"Appending Gaps, periodType: $periodType, series column:$columnSerie")
    var dateLen=0;//largo que va a tener la cadena de string segun el tipo de periodo elegido
    var period:Period=null
    periodType match {
      case "A" => 
        dateLen=4
        period=Period.years(step)
      case "M" => 
        dateLen=6
        period=Period.months(step)
      case "D" => 
        dateLen=8
        period=Period.days(step)
      case "h" => 
        dateLen=9
        period=Period.hours(step)
      case "m" => 
        dateLen=11
        period=Period.minutes(step)
      case "s" => 
        dateLen=13
        period=Period.seconds(step)

    }
    
    
    //tengo que proyectar la columna con la fecha pero tambien, la columna con el valor de corte si tiene
    /*var columnSeq:Seq[String] = null
    if (!breakColumn.isEmpty)
      columnSeq = Seq(columnSerie, breakColumn.get)
    else
      columnSeq = Seq(columnSerie)
      
    //tengo que proyectar el Id que luego se usa par hacer el join para replicar los datos anteriores
    columnSeq = columnSeq.union(idColumns.filter { idCx => columnSeq.contains(idCx)} toSeq )
    */
    import org.apache.spark.sql.functions._
    val dfDatesFilter = df.orderBy(Seq(desc(columnSerie)).union( orderColumns.map { col =>
                      
              if (!df.columns.exists { nameCol => nameCol == col.columnName })
                throw new Exception(s"Column ${col.columnName} not exists, options: ${df.columns.mkString(",")}")

              col.order.toUpperCase() match {

                case "DESC" => desc(col.columnName)
                case "ASC"  => asc(col.columnName)
                case _      => throw new Exception(s"Operation ${col.order.toUpperCase()} not valid, options: DESC, ASC")
              }
            } toSeq) : _*).collect() 
    val datesFilter = dfDatesFilter
      .map{ 
            m=> {
              if ( m.get(m.fieldIndex(columnSerie)).isInstanceOf[Timestamp] ) 
                new DateRangeData(m.getTimestamp(m.fieldIndex(columnSerie)), m)
              else
                if ( m.get(m.fieldIndex(columnSerie)).isInstanceOf[Date] ) 
                  new DateRangeData(m.getDate(m.fieldIndex(columnSerie)).getTime(), m)
                else 
                  new DateRangeData(null, m)
            }
          } toSeq
    
    var dateToLocal:DateTime = null
    
    if (!breakColumn.isEmpty )
      dateToLocal = getDateToFromDF( dfDatesFilter, breakColumn.get, breakValue.getOrElse(null), columnSerie )
    
    if (dateToLocal ==null)
      dateToLocal = dateTo
      
      
    var dr:DateRange = new DateRange( dateFrom, dateToLocal, period,true,
        datesFilter, dateLen,periodType );
   
    import sqlContext.implicits._
    //solo para obtener la estructura tomo este primer registro
    val firstDFRecor = df.head();
    var p = sc.sparkContext.parallelize(
    dr.map { x => 
      
      val seqFields:Seq[Any] = df.schema.fields.map {
        f => if (f.name.equals(columnSerie)) 
              {
                f.dataType match{
                  case DateType => new java.sql.Date(x.date.asInstanceOf[DateTime].getMillis)
                  case TimestampType => new Timestamp(x.date.asInstanceOf[DateTime].getMillis) 
                  case _ => new Timestamp(x.date.asInstanceOf[DateTime].getMillis)
                }
              }
             else{
               x.data match {
                 case _:Row => 
                              if (x.virtual)
                               if (replicateColumns.exists { _==f.name  }) x.data.get( x.data.fieldIndex(f.name) ) else null
                              else
                                x.data.get( x.data.fieldIndex(f.name) )
                 case null =>{
                     if(splitColumns.contains(f.name) )
                       firstDFRecor.get( firstDFRecor.fieldIndex(f.name) )
                     else
                       null
                 }
               }
             }
        } toSeq
       var r = Row.fromSeq(seqFields)
       r
      } toSeq    , 4
      )
  
   p.cache()
   p
  }
 
  def getDateToFromDF( datesFilter:Array[Row], breakColumn:String, breakValue:Iterable[String], dateTimeColumn:String ):DateTime ={
      if (breakValue==null)
        null
      val row = datesFilter.head
      val lastRowValue = row.get( row.fieldIndex(breakColumn) ).toString()
      if (lastRowValue!=null && breakValue.exists { _.equals(lastRowValue) } )
        {
          val value = row.get( row.fieldIndex(dateTimeColumn) )
          new DateTime(value)
            
         }
      else
        null
  }
  
}