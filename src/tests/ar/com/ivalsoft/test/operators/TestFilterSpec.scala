
package ar.com.ivalsoft.test.operators

import java.util.concurrent.TimeUnit

import ar.com.ivalsoft.etl.executor.filter.Filter
import ar.com.ivalsoft.etl.executor.filter.params.FilterParams
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
class TestFilterSpec extends GeneralSpec {

  implicit val sc =sconf.getOrCreate()
  implicit val ec = ExecutionContext.global
    
  "Filter" should
  {
    
   "and" in {
    
     
      val executor1 = new SQLDataReader
      
      executor1.param = this.getTestDb1TableConnection()
      val e1 = executor1.execute
      val res = Await.result( e1 ,
          Duration.apply(2, TimeUnit.MINUTES)) 

      res must beSome
      
      val filter= new Filter()
      filter.dfListIn = Seq(e1)
        
      filter.param = new FilterParams(
      
          "code=1 and text1 like(\"%spa%\")"
      
      )
      
      val w2 = filter.execute
      
      val res2 = Await.result( w2 ,
          Duration.apply(2, TimeUnit.MINUTES)) 
      
      res2 must beSome
      
      res2.get.count() === 1

    }
   
   "SQL Filter or" in {
    
     
      val executor1 = new SQLDataReader
      
      executor1.param = this.getTestDb1TableConnection()
      
      val e1 = executor1.execute
      val res = Await.result( e1 ,
          Duration.apply(2, TimeUnit.MINUTES)) 

      res must beSome
      val filter= new Filter()
      
      filter.dfListIn = Seq(e1)
      filter.param = new FilterParams(
            "code=2 or text1=\"space     s     ss\""
      )
      
      val w2 = filter.execute
      
      val res2 = Await.result( w2 ,
          Duration.apply(2, TimeUnit.MINUTES)) 
      
      res2 must beSome
      
      res2.get.count()===3

    }
  
    "SQL Filter parenthesis" in {
    
     
      val executor1 = new SQLDataReader
      
      executor1.param =this.getTestDb1TableConnection()
      val e1 = executor1.execute
      val res = Await.result( e1 ,
          Duration.apply(2, TimeUnit.MINUTES)) 

      res must beSome
      val filter= new Filter()
      filter.dfListIn = Seq(e1)
      filter.param = new FilterParams(
       "( substring(text1,0,2)='sp' )"
      )
      
      val w2 = filter.execute
      val res2 = Await.result( w2 ,
          Duration.apply(2, TimeUnit.MINUTES)) 
      
      res2 must beSome
      
      res2.get.count() === 1

    }
  }
   
}
