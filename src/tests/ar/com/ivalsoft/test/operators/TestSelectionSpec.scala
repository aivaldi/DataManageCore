
package ar.com.ivalsoft.test.operators

import java.util.concurrent.TimeUnit

import ar.com.ivalsoft.etl.executor.params.ColumnAlias
import ar.com.ivalsoft.etl.executor.preparation.SelectionPreparation
import ar.com.ivalsoft.etl.executor.preparation.params.SelectionPreparationParams
import ar.com.ivalsoft.etl.executor.reader.SQLDataReader
import ar.com.ivalsoft.test.GeneralSpec
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class TestSelectionSpec extends GeneralSpec {
   implicit val sc =sconf.getOrCreate()
  
    implicit val ec = ExecutionContext.global
    
  "Selection Executor" should
  {
    
   "SQL Source  Selection df alias" in {
    
     
      val executor1 = new SQLDataReader
      
      executor1.param = this.getTestDb1TableConnection()
      val w = executor1.execute
      val res = Await.result( w ,
          Duration.apply(2, TimeUnit.MINUTES)) 
            
      
      val dfo1 = res
      
      dfo1 must beSome
      
      val selPreparation = new SelectionPreparation()
      
      selPreparation.dfListIn = Seq(w)
        
      
      val params = new SelectionPreparationParams(
          Seq( ColumnAlias("quantity", "quantity22"),ColumnAlias("val", null)
              ))
     
      selPreparation.param = params;
      
      
      val w2 = selPreparation.execute
      
      val res2 = Await.result( w2 ,
          Duration.apply(2, TimeUnit.MINUTES)) 
      
      res2 must beSome
          
      res2.get.columns.length must be_==(2)

      res2.get.columns.toSeq.exists { _ == "quantity22" } must beTrue
      res2.get.columns.toSeq.exists { _ == "val" } must beTrue
      

    }

  }
   
}
