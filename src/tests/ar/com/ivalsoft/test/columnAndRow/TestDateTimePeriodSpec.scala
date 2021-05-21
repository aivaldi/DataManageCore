
package ar.com.ivalsoft.test.columnAndRow

import java.util.concurrent.TimeUnit

import ar.com.ivalsoft.etl.executor.function.params.DateTimeDetailColumnParams
import ar.com.ivalsoft.etl.executor.reader.SQLDataReader
import ar.com.ivalsoft.etl.executor.udf.DateTimePeriodUDF
import ar.com.ivalsoft.spark.source.parser.sql.SQLParams
import ar.com.ivalsoft.test.GeneralSpec
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.specs2.mutable._
import org.specs2.runner.JUnitRunner

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class TestDateTimePeriodSpec extends GeneralSpec {

  implicit val sc =sconf.getOrCreate()
  
   DateTimePeriodUDF.register(sc)
    implicit val ec = ExecutionContext.global
    
  "Date Time Period" should
  {
    
   "Create" in {
    
     
      val executor1 = new SQLDataReader
      
      executor1.param = this.getTestDb1TableConnection()

      val e1 = executor1.execute
      val res = Await.result( e1 ,
          Duration.apply(2, TimeUnit.MINUTES)) 

      res must beSome
      
      val cc= new ar.com.ivalsoft.etl.executor.function.DateTimeDetailColumn
      
      cc.dfListIn = Seq(e1)
        
      cc.param = new DateTimeDetailColumnParams(Seq("Divide by biometry","dateString"),
          "substring(dateString,0,10)","yyyy-MM-dd",Seq("Bi: P2 Tr: P3", "YYYY MM dd")
      )
      
      val w2 = cc.execute
      
      val res2 = Await.result( w2 ,
          Duration.apply(2, TimeUnit.MINUTES)) 
      
      res2 must beSome
      val res2real = res2.get
     res2real.show()
     res2real.columns.length === 8
     res2real.collect().head(7) === "Bi: 1 Tr: 1"

    }
      
  }
   
}
