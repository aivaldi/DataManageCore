
package ar.com.ivalsoft.test.columnAndRow

import java.util.concurrent.TimeUnit

import ar.com.ivalsoft.etl.executor.function.params.FillGapDateTimeFunctionParams
import ar.com.ivalsoft.etl.executor.reader.SQLDataReader
import ar.com.ivalsoft.spark.source.parser.sql.SQLParams
import ar.com.ivalsoft.test.GeneralSpec
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.specs2.mutable._
import org.specs2.runner.JUnitRunner

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class FillGapDataTimeExecutorsSpec extends GeneralSpec {

  implicit val sc =sconf.getOrCreate()
    implicit val ec = ExecutionContext.global
    
  "Proration" should
  {
    
   "1 partition" in {
    
     
      val executor1 = new SQLDataReader
      
      executor1.param =this.getTestDb1TableConnection()

      val e1 = executor1.execute

      val res = Await.result( e1 ,
          Duration.apply(2, TimeUnit.MINUTES)) 

      res must beSome
      
      val cc= new ar.com.ivalsoft.etl.executor.function.FillGapsDateTime
      
      cc.dfListIn = Seq(e1)
        
      cc.param = new FillGapDateTimeFunctionParams(
          Seq("code","val","quantity"),"dateString",Seq("quantity"),Seq(),
          "D",1,DateTime.now().minusMonths(6), 
          DateTime.now().minusMonths(6).plusDays(20), "not used", None, None
      )
      
      val w2 = cc.execute
      
      val res2 = Await.result( w2 ,
          Duration.apply(2, TimeUnit.MINUTES)) 
      
      res2 must beSome
      
      res2.get.orderBy("code","dateString").show(200)
      
      1===1
    }
      
  }
   
}
