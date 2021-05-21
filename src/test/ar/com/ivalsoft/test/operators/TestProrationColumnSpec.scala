
package ar.com.ivalsoft.test.operators

import java.util.concurrent.TimeUnit

import ar.com.ivalsoft.etl.executor.function.params.ProrationFunctionParams
import ar.com.ivalsoft.etl.executor.reader.SQLDataReader
import ar.com.ivalsoft.test.GeneralSpec
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class TestProrationColumnSpec extends GeneralSpec {
   implicit val sc =sconf.getOrCreate()
   implicit val ec = ExecutionContext.global
    
  "Proration" should
  {
    
   "by 1 partition" in {
    
     
      val executor1 = new SQLDataReader
      
      executor1.param = this.getTestDb1TableConnection()
      val e1 = executor1.execute
      val res = Await.result( e1 ,
          Duration.apply(2, TimeUnit.MINUTES)) 
            
      
      res must beSome
      
      val cc= new ar.com.ivalsoft.etl.executor.function.ProrationColumn
      
      cc.dfListIn = Seq(e1)
        
      cc.param = new ProrationFunctionParams(
          Seq("code"),
          "quantity",
          "val",
          "Proration"
      )
      
      val w2 = cc.execute
      
      val res2 = Await.result( w2 ,
          Duration.apply(2, TimeUnit.MINUTES)) 
      
      res2 must beSome
      
      res2.get.show()
      
      1===1
    }
      
  }
   
}
