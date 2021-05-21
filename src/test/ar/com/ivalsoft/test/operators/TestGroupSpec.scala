
package ar.com.ivalsoft.test.operators

import java.util.concurrent.TimeUnit

import ar.com.ivalsoft.etl.executor.group.params.GroupParams
import ar.com.ivalsoft.etl.executor.params.ColumnAggregated
import ar.com.ivalsoft.etl.executor.reader.SQLDataReader
import ar.com.ivalsoft.test.GeneralSpec
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class TestGroupSpec extends GeneralSpec {

    
  implicit val sc =sconf.getOrCreate()
    implicit val ec = ExecutionContext.global
    
  "Group" should
  {
    

   "1 aggregations" in {
      val executor1 = new SQLDataReader
      
      executor1.param = this.getTestDb1TableConnection()
      val e1 = executor1.execute
      val res = Await.result( e1 ,
          Duration.apply(2, TimeUnit.MINUTES)) 

      res must beSome
      val order= new ar.com.ivalsoft.etl.executor.group.Group()
      
      order.dfListIn = Seq(e1)
      order.param = new GroupParams(
          Seq("code"),
          Seq(
            new ColumnAggregated("val", "val","sum")
          )
      )
      
      val w2 = order.execute
     val res2 = Await.result( w2 ,
       Duration.apply(2, TimeUnit.MINUTES))


      res2 must beSome

      res2.get.count() === 3

      res2.get.collect()(0)(1).toString === 22.toString()
     res2.get.collect()(1)(1).toString === 112.toString()
     res2.get.collect()(2)(1).toString  === 63.toString()
    }
 
   
  }
   
}
