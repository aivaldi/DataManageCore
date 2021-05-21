package ar.com.ivalsoft.etl.executor.function.helper
import org.joda.time.{ Period, DateTime }
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import java.sql.Timestamp
import java.sql.Date
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.joda.time.Years
import org.joda.time.Months


case class DateRangeData (date:Any, data:Row, virtual:Boolean=false)

/**
 * @author aivaldi
 * Genera un rango de datos como iterador.
 */
class DateRange(val start: DateTime, val end: DateTime,
                val step: Period, inclusive: Boolean, 
                dateRangeDataSeq: Seq[DateRangeData], exludeLength: Int, perdio:String) 
                extends Iterable[DateRangeData] {

  val log = Logger.getLogger(getClass.getName)

  override def iterator: Iterator[DateRangeData] = new DateRangeIterator

  class DateRangeIterator extends Iterator[DateRangeData] {
    val log = Logger.getLogger(getClass.getName)
    
    var current = start
    var lastDateFounded:DateRangeData =new DateRangeData(null, null)

    override def hasNext: Boolean = current.isBefore(end) || (inclusive && current == end)

    override def next(): DateRangeData = {
     
      if (dateRangeDataSeq.exists {
        x =>
          if (x == null) {
            log.debug("No value found for date inferred")
            false
          } else {
            var iteratedDate:DateTime = null
            x.date match {
              case l: java.lang.Long => iteratedDate = new DateTime(l).withMillisOfDay(0) 
              case ts:Timestamp => iteratedDate = new DateTime(ts.getTime).withMillisOfDay(0)
              case _ => {
                          iteratedDate=null
                        }
            }
            if (iteratedDate==null)
              false
            else  
              if( comparePeriodDate( iteratedDate, current,perdio))
                {  
                  lastDateFounded = new DateRangeData(current.withMillisOfDay(0), x.data)
                  true
                }
              else
                false
            
          }
        })
          { //si esta excluido entonces lo paso por alto y traigo el proximo
            current = current.withPeriodAdded(step, 1)
            lastDateFounded
          } 
          else 
          {
            val returnVal = current.withMillisOfDay(0)
            current = current.withPeriodAdded(step, 1)
            new DateRangeData(returnVal,lastDateFounded.data, true )
          }
    }

      /**
     * Dado un periodo comparo con los valores que corresponda
     */
    def comparePeriodDate( date:DateTime , dateCompare:DateTime ,period:String):Boolean={
      
      period match {
         case "A" => 
          date.getYear.equals(dateCompare.getYear)
        case "M" => 
          date.getYear.equals(dateCompare.getYear) && date.getMonthOfYear.equals(dateCompare.getMonthOfYear)
        case "D" => 
          var local = dateCompare.toLocalDate().toDateTimeAtStartOfDay()
          date.toLocalDate().toDateTimeAtStartOfDay().equals(local)
        case "h" => 
          date.equals(dateCompare.withMillisOfDay(0))
        case "m" => 
          date.equals(dateCompare.withMillisOfDay(0))
        case "s" => 
          date.equals(dateCompare.withMillisOfDay(0))
        }
      
    }
  
  }
  

}