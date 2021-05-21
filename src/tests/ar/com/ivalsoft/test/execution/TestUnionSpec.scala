
package ar.com.ivalsoft.test.execution

import java.util.concurrent.TimeUnit

import ar.com.ivalsoft.etl.executor.join.Union
import ar.com.ivalsoft.etl.executor.join.params.UnionParams
import ar.com.ivalsoft.etl.executor.reader.SQLDataReader
import ar.com.ivalsoft.spark.source.parser.sql.SQLParams
import ar.com.ivalsoft.test.GeneralSpec
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.specs2.mutable._
import org.specs2.runner.JUnitRunner

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class TestUnionSpec extends GeneralSpec {

    
  implicit val sc =sconf.getOrCreate()
    implicit val ec = ExecutionContext.global
    
  "Union " should
  {
    
   "test" in {


     val executor1 = new SQLDataReader
     val executor2 = new SQLDataReader

     executor1.param = this.getTestDb1TableConnection()

     executor2.param = this.getTestDb2TableConnection()

     val e1 = executor1.execute
     val e2 = executor2.execute

     val w = Future.sequence(Seq(e1, e2))

     val res = Await.result(w,
       Duration.apply(2, TimeUnit.MINUTES))


     val dfo1 = res(0)

     dfo1 must beSome

     val dfo2 = res(1)

     dfo1 must beSome

     val joinPreparation = new Union()

     joinPreparation.dfListIn = Seq(e1, e2)

     joinPreparation.param = new UnionParams(
       Seq("val", "quantity"),
       Seq("val", "quantity"),
       Some(Seq("article")),
       Some(Seq("company"))
     )


     val w2 = joinPreparation.execute

     val res2 = Await.result(w2,
       Duration.apply(2, TimeUnit.MINUTES))

     res2 must beSome

     res2.get.columns.length must be_==(2)
   }
  
  }
   
}
