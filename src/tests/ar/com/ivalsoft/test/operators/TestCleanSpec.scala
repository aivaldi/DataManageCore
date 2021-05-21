
package ar.com.ivalsoft.test.operators

import java.util.concurrent.TimeUnit

import ar.com.ivalsoft.etl.executor.preparation.CleaningPreparation
import ar.com.ivalsoft.etl.executor.preparation.params.{CleaningParam, CleaningPreparationParams}
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
class TestCleanSpec extends GeneralSpec {

    
  implicit val sc =sconf.getOrCreate()
    implicit val ec = ExecutionContext.global
    
  "Cleaning" should
  {
    
   "test" in {

      val executor1 = new SQLDataReader
      
      executor1.param =this.getTestDb1TableConnection()

      val w = executor1.execute
      
      val res = Await.result( w ,
          Duration.apply(2, TimeUnit.MINUTES)) 
            
      
      val dfo1 = res
      
      dfo1 must beSome
      
      val cleaningPreparation = new CleaningPreparation()
      
      cleaningPreparation .dfListIn = Seq(w)
        
      
      val params = new CleaningPreparationParams(
          Seq( 
              CleaningParam("text13", "trim(regexp_replace(text1,'(\\\\s+)','') )" ),
              CleaningParam("text2", "trim(if( isnull(text2)=1,'',regexp_replace(text2,'\\\\s+',' ') ))" )
           )
      )
      cleaningPreparation.param = params;
      
      
      val w2 = cleaningPreparation .execute
      
      val res2 = Await.result( w2 ,
          Duration.apply(2, TimeUnit.MINUTES)) 
      
      
      res2.get.show()
      
      res2 must beSome
          
      res2.get.columns.length must be_==(8)


    }
   
   
  }
   
}
