
package ar.com.ivalsoft.test.operators

import java.util.concurrent.TimeUnit

import ar.com.ivalsoft.etl.executor.preparation.DynamicTablePreparation
import ar.com.ivalsoft.etl.executor.preparation.params.{DynamicTableFieldsParams, DynamicTableParams}
import ar.com.ivalsoft.test.GeneralSpec
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.specs2.mutable._
import org.specs2.runner.JUnitRunner

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class TestDynamicSpec extends GeneralSpec {

  implicit val sc =sconf.getOrCreate()
    implicit val ec = ExecutionContext.global
    
  "Dynamic " should
  {
    
   "Dynamic Table" in {
    
     
      val executor1 = new DynamicTablePreparation
      
      executor1.param = DynamicTableParams(
        Seq( 
            DynamicTableFieldsParams("title", "string", false),
            DynamicTableFieldsParams("value", "int", false) 
            )
            ,Seq(
              Seq("Title one", 0),
              Seq("Title two", 1),
              Seq("Title tree", 2)
            )
      )

      val w = executor1.execute
      val res = Await.result( w ,
          Duration.apply(2, TimeUnit.MINUTES)) 
      val dfo1 = res
      
      dfo1 must beSome
      val dfo1Real = dfo1.get
      dfo1Real.show()
      dfo1Real.columns.length === 2
      dfo1Real.count() === 3


    }
        
  }
   
}
