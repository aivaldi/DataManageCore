package ar.com.ivalsoft.etl.executor.udf

import org.apache.spark.sql.SQLContext
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import org.apache.spark.sql.SparkSession

/**
 * @author aivaldi
 */
object DateTimePeriodUDF {


  
  def register(sc:SparkSession) = {
      
      sc.udf.register("timePeriod", (date:String, dateFormat:String, periodExp:String) => 
        {
           if (date==null)
           {
             ""
           }
           else{
             var dt = DateTimeFormat.forPattern(dateFormat).parseDateTime(date);
             var ret = periodExp
             //hago los mapeos necesarios
             if (periodExp.indexOf("P1")!= -1 )
             {
                 ret = ret.replace("P1", dt.getMonthOfYear().toString)
             }
             if (periodExp.indexOf("P2")!= -1 )
             {
                 ret = ret.replace("P2", math.ceil(dt.getMonthOfYear()/2d).toInt.toString)
             }
             if (periodExp.indexOf("P3")!= -1 )
             {
                 ret = ret.replace("P3", math.ceil(dt.getMonthOfYear()/3d).toInt.toString)
             }
             if (periodExp.indexOf("P4")!= -1 )
             {
                 ret = ret.replace("P4", math.ceil(dt.getMonthOfYear()/4d).toInt.toString)
             }
             if (periodExp.indexOf("P6")!= -1 )
             {
                 ret = ret.replace("P6", math.ceil(dt.getMonthOfYear()/6d).toInt.toString)
             }
             
             
             if (periodExp.indexOf("YYYY")!= -1 )
             {
                 ret = ret.replace("YYYY", dt.getYear().toString)
             }
             
             if (periodExp.indexOf("dd")!= -1 )
             {
                 ret = ret.replace("dd", if (dt.getDayOfMonth().toString.length()==1)"0"+dt.getDayOfMonth().toString else dt.getDayOfMonth().toString )
             }
             if (periodExp.indexOf("dy")!= -1 )
             {
                 ret = ret.replace("dy", dt.getDayOfYear().toString)
             }
             if (periodExp.indexOf("dw")!= -1 )
             {
                 ret = ret.replace("dw", dt.getDayOfWeek().toString)
             }
             
             if (periodExp.indexOf("MM")!= -1 )
             {
                 ret = ret.replace("MM", if (dt.getMonthOfYear().toString.length()==1)"0"+dt.getMonthOfYear().toString else dt.getMonthOfYear().toString )
             }
             
             if (periodExp.indexOf("WW")!= -1 )
             {
                 ret = ret.replace("WW", (dt.getDayOfMonth() % 7).toString() )
             }
             
             if (periodExp.indexOf("WY")!= -1 )
             {
                 ret = ret.replace("WY", (dt.getWeekyear).toString())
             }
             
             ret
           }
           
        }
      )
      
    } 
  
  
  
}

