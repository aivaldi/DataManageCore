
package ar.com.ivalsoft.test.operators

import java.util.concurrent.TimeUnit

import ar.com.ivalsoft.etl.executor.function.IncrementalColumnsPreparation
import ar.com.ivalsoft.etl.executor.function.params.IncrementalPreparationParams
import ar.com.ivalsoft.etl.executor.params.ColumnOrderPair
import ar.com.ivalsoft.etl.executor.reader.SQLDataReader
import ar.com.ivalsoft.test.GeneralSpec
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class TestIncrementalPreparationSpec extends GeneralSpec {

    
  implicit val sc =sconf.getOrCreate()
    implicit val ec = ExecutionContext.global
    
  "Incremental Executor" should
  {
    
   "SQL Source  Incremental" in {
    
     
      val executor1 = new SQLDataReader
      
      executor1.param = this.getTestDb1TableConnection()
      
      val w = executor1.execute
      val res = Await.result( w ,
          Duration.apply(2, TimeUnit.MINUTES)) 
      val dfo1 = res
      
      dfo1 must beSome
      val incPreparation = new IncrementalColumnsPreparation()
      
      incPreparation.dfListIn = Seq(w)
      val params = new IncrementalPreparationParams( Seq(),null,
        "id2", Seq(ColumnOrderPair("code","DESC"), ColumnOrderPair("dateString","DESC")))
     
      incPreparation.param = params;
      
      
      val w2 = incPreparation.execute
      
      val res2 = Await.result( w2 ,
          Duration.apply(2, TimeUnit.MINUTES)) 
      
      res2 must beSome
          
      res2.get.columns.length must be_==(8)

      res2.get.columns.toSeq.exists { _ == "id2" } must beTrue

    }
   
   
  }
   
}
