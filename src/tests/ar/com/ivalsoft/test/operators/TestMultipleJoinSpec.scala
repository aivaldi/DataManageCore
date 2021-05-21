
package ar.com.ivalsoft.test.operators

import java.util.concurrent.TimeUnit

import ar.com.ivalsoft.etl.executor.join.MultipleJoin
import ar.com.ivalsoft.etl.executor.join.params.{MultipleJoinParams, TableJoin}
import ar.com.ivalsoft.etl.executor.params.ColumnAlias
import ar.com.ivalsoft.etl.executor.reader.SQLDataReader
import ar.com.ivalsoft.test.GeneralSpec
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class TestMultipleJoinSpec extends GeneralSpec {

    
  implicit val sc =sconf.getOrCreate()
    implicit val ec = ExecutionContext.global
    
  "Join Multiple" should
  {
    
   "join" in {
    
      val executor1 = new SQLDataReader
      val executor2 = new SQLDataReader
      
       executor1.param = this.getTestDb1TableConnection()
      
      executor2.param = this.getTestDb2TableConnection()
      
      val e1 = executor1.execute
      val e2 = executor2.execute
      
      val w = Future.sequence(Seq(e1,e2))
      
      val res = Await.result( w ,
          Duration.apply(2, TimeUnit.MINUTES)) 
            
      
      val dfo1 = res(0)
      
      dfo1 must beSome
      
      val dfo2 = res(1)
      
      dfo1 must beSome
      
      val joinPreparation = new MultipleJoin()
      
      joinPreparation.dfListIn = Seq(e1, e2)
        
      joinPreparation.param = new MultipleJoinParams(
                  "t1",
                  Seq ( TableJoin("t2", "INNER JOIN", "t1.dateString=t2.dateString and t1.code=t2.code")),
                  Seq( 
                      new ColumnAlias("code", "code1", Some("t1")),
                      new ColumnAlias("dateString", "dateString1", Some("t1")),
                      new ColumnAlias("code", "code2", Some("t2")),
                      new ColumnAlias("dateString", "dateString2", Some("t2"))
                      )
                  )
                  
      
      val w2 = joinPreparation.execute
      

      val res2 = Await.result( w2 ,
          Duration.apply(2, TimeUnit.MINUTES)) 

      res2 must beSome

      res2.get.show()

     res2.get.columns.contains("code1") === true
     res2.get.columns.contains("code2") === true
     res2.get.columns.contains("dateString1") === true
     res2.get.columns.contains("dateString2") === true

    }
  
  }
   
}
