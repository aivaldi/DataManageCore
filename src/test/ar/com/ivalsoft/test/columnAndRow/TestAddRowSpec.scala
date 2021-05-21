
package ar.com.ivalsoft.test.columnAndRow

import java.util.concurrent.TimeUnit

import ar.com.ivalsoft.etl.executor.function.AddRows
import ar.com.ivalsoft.etl.executor.function.params.{AddRowParams, CalculatedColumnParams}
import ar.com.ivalsoft.etl.executor.reader.SQLDataReader
import ar.com.ivalsoft.spark.source.parser.sql.SQLParams
import ar.com.ivalsoft.test.GeneralSpec
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class TestAddRowSpec extends GeneralSpec {

    
    
  implicit val sc =sconf.getOrCreate()
    implicit val ec = ExecutionContext.global
    
  "AddRow Executor" should
  {
    
   "Add one row" in {
    
     
      val executor1 = new SQLDataReader
      
      executor1.param = this.getTestDb3TableConnection()
      
      
      val e1 = executor1.execute
      
      val res = Await.result( e1 ,
          Duration.apply(2, TimeUnit.MINUTES)) 
            
      
      res must beSome
      
      
      val cc= new ar.com.ivalsoft.etl.executor.function.CalculatedColumn
      
      cc.dfListIn = Seq(e1)
        
      cc.param = new CalculatedColumnParams(
          "CC1",
          ("format_number(jan,2)")
      )
      
      val w2 = cc.execute
      
      val res3 = Await.result( w2 ,
          Duration.apply(2, TimeUnit.MINUTES)) 
      
      
      val addRow= new AddRows()
      
      addRow.dfListIn = Seq(w2)
        
      addRow.param = new AddRowParams(
        Seq( Seq(0,"99",22,55,"12","12","222"  ) )
      )
      val w3 = addRow.execute
      
      val res2 = Await.result( w3 ,
          Duration.apply(2, TimeUnit.MINUTES)) 
      
      res2 must beSome

      val rdd2real = res2.get

     rdd2real.columns.contains("CC1") === true

     rdd2real.count() === 3

    }
   
 
  }
   
}
