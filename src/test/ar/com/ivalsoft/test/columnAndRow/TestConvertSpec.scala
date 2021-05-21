
package ar.com.ivalsoft.test.columnAndRow

import java.util.concurrent.TimeUnit

import ar.com.ivalsoft.etl.executor.params.ColumnAlias
import ar.com.ivalsoft.etl.executor.preparation.ConvertPreparation
import ar.com.ivalsoft.etl.executor.preparation.params.ConvertPreparationParams
import ar.com.ivalsoft.etl.executor.reader.SQLDataReader
import ar.com.ivalsoft.spark.source.parser.sql.SQLParams
import ar.com.ivalsoft.test.GeneralSpec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StringType}
import org.junit.runner.RunWith
import org.specs2.mutable._
import org.specs2.runner.JUnitRunner

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class TestConvertSpec extends GeneralSpec {

  implicit val sc =sconf.getOrCreate()
    implicit val ec = ExecutionContext.global
    
  "Convert" should
  {
    
   "execute" in {
    
     
      val executor1 = new SQLDataReader
      
      executor1.param = this.getTestDb1TableConnection()

     val w = executor1.execute
      val res = Await.result( w ,
          Duration.apply(2, TimeUnit.MINUTES)) 

      val dfo1 = res
      
      dfo1 must beSome
      
      val selPreparation = new ConvertPreparation()
      
      selPreparation.dfListIn = Seq(w)
        
      
      val params = new ConvertPreparationParams(
          Seq( ColumnAlias("count", "", None ,Some("Decimal(18,3)")),
          ColumnAlias("dateString", "", None ,Some("StringType")))
          )
     
      selPreparation.param = params;

      val w2 = selPreparation.execute
      
      val res2 = Await.result( w2 ,
          Duration.apply(2, TimeUnit.MINUTES)) 
      
      res2 must beSome
          
      
      res2.get.show()
      
      res2.get.columns.length must be_==(7)
      
      
      res2.get.columns.foreach { println }
      
      res2.get.printSchema();

     res2.get.schema.last.dataType === StringType
      
      1===1
    }
   
     
  }
   
}
