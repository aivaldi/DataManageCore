
package ar.com.ivalsoft.test.columnAndRow

import java.util.concurrent.TimeUnit

import ar.com.ivalsoft.etl.executor.function.params.CalculatedColumnParams
import ar.com.ivalsoft.etl.executor.reader.SQLDataReader
import ar.com.ivalsoft.spark.source.parser.sql.SQLParams
import ar.com.ivalsoft.test.GeneralSpec
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.specs2.mutable._
import org.specs2.runner.JUnitRunner

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class TestCalcColumnSpec extends GeneralSpec {

    
  implicit val sc =sconf.getOrCreate()
  
    implicit val ec = ExecutionContext.global
    
  "Calc specs" should
  {
    
   "add 1" in {
    
     
      val executor1 = new SQLDataReader

     executor1.param = this.getTestDb1TableConnection()
      
      val e1 = executor1.execute

      val res = Await.result( e1 ,
          Duration.apply(2, TimeUnit.MINUTES)) 

      res must beSome
      
      val cc= new ar.com.ivalsoft.etl.executor.function.CalculatedColumn
      
      cc.dfListIn = Seq(e1)
        
      cc.param = new CalculatedColumnParams(
          "CC1",
          ("if(code=1, 'code1','other')")
      )
      
      val w2 = cc.execute
      
      val res2 = Await.result( w2 ,
          Duration.apply(2, TimeUnit.MINUTES)) 
      
      res2 must beSome
      
      res2.get.show()
      val rest2Real = res2.get
      rest2Real.columns.contains("CC1") === true
    }

   
  }
   
}
