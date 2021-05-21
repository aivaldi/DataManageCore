
package ar.com.ivalsoft.test.columnAndRow

import java.util.concurrent.TimeUnit

import ar.com.ivalsoft.etl.executor.function.params.WindowFunctionParams
import ar.com.ivalsoft.etl.executor.params.ColumnOrderPair
import ar.com.ivalsoft.etl.executor.reader.SQLDataReader
import ar.com.ivalsoft.test.GeneralSpec
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class TestAccColSpec extends GeneralSpec {


  implicit val sc =sconf.getOrCreate()
    implicit val ec = ExecutionContext.global
    
  "Acc test " should
  {
    
   "1 partition" in {
    
     
      val executor1 = new SQLDataReader
      
      executor1.param = this.getTestDb1TableConnection()
      val e1 = executor1.execute
      val res = Await.result( e1 ,
          Duration.apply(2, TimeUnit.MINUTES))

     res must beSome
     val cc= new ar.com.ivalsoft.etl.executor.function.AccumulatorColumn

     cc.dfListIn = Seq(e1)
        
      cc.param = new WindowFunctionParams(
          Seq("code"),
          Seq( ColumnOrderPair("val","DESC") ),
          "quantity",
          "accCount"
      )
      
      val w2 = cc.execute
      
      val res2 = Await.result( w2 ,
          Duration.apply(2, TimeUnit.MINUTES)) 
      
      res2 must beSome
      
      val res2Selected = res2.get.select("code","quantity","accCount")

     res2Selected show()

     res2Selected count() mustEqual(6)

     res2Selected.collect()(1)(2) mustEqual 3
     res2Selected.collect()(3)(2) mustEqual 17
     res2Selected.collect()(5)(2) mustEqual 5

    }
      
  }
   
}
