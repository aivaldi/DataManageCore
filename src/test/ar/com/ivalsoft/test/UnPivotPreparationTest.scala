
package ar.com.ivalsoft.test

import org.specs2.mutable._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import scala.concurrent.{ Future, Promise }
import scala.concurrent.ExecutionContext
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import java.sql.Time
import ar.com.ivalsoft.etl.executor.reader.SQLDataReader
import ar.com.ivalsoft.spark.source.persister.sql.SQLConnection
import ar.com.ivalsoft.spark.source.persister.SourcePersister
import ar.com.ivalsoft.spark.source.persister.sql.SQLPersister
import ar.com.ivalsoft.spark.source.parser.sql.SQLParams
import ar.com.ivalsoft.etl.executor.preparation.SelectionPreparation
import ar.com.ivalsoft.etl.executor.params.ColumnAlias
import ar.com.ivalsoft.etl.executor.preparation.params.UnPivotPreparationParams
import ar.com.ivalsoft.etl.executor.preparation.params.UnPivotPreparationParams
import ar.com.ivalsoft.etl.executor.preparation.UnPivotPreparation
import org.apache.spark.sql.SparkSession

@RunWith(classOf[JUnitRunner])
class TestUnPivotPreparationSpec extends GeneralSpec {

  implicit val sc =sconf.getOrCreate()
    implicit val ec = ExecutionContext.global
    
  "UnPivot Executor" should
  {
    
   "SQL Source  Selection df alias" in {
    
     
      val executor1 = new SQLDataReader
      
      executor1.param = this.getTestDb3TableConnection()
      
      
      val w = executor1.execute
      
      val res = Await.result( w ,
          Duration.apply(2, TimeUnit.MINUTES)) 
            
      
      val dfo1 = res
      
      dfo1 must beSome
      
      val selPreparation = new UnPivotPreparation()
      
      selPreparation.dfListIn = Seq(w)
        
      
      val params = new UnPivotPreparationParams("month", Seq("jan", "feb", "march", "april"), "values")
      selPreparation.param = params;
      
      
      val w2 = selPreparation.execute
      
      val res2 = Await.result( w2 ,
          Duration.apply(2, TimeUnit.MINUTES)) 
      
      res2 must beSome

      res2.get.columns.length must be_==(4)

      res2.get.columns.toSeq.exists { _ == "month" } must beTrue
      res2.get.columns.toSeq.exists { _ == "values" } must beTrue
      

      
      
      1===1
    }
   
  
  
  }
   
}
