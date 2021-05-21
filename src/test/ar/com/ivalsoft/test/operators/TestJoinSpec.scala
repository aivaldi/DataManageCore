
package ar.com.ivalsoft.test.operators

import java.util.concurrent.TimeUnit

import ar.com.ivalsoft.etl.executor.join.Join
import ar.com.ivalsoft.etl.executor.join.params.JoinParams
import ar.com.ivalsoft.etl.executor.params.{ColumnAlias, ColumnClausePair}
import ar.com.ivalsoft.etl.executor.reader.SQLDataReader
import ar.com.ivalsoft.test.GeneralSpec
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class TestJoinSpec extends GeneralSpec {

  implicit val sc =sconf.getOrCreate()
    implicit val ec = ExecutionContext.global
    
  "Join" should
  {
    
   "test" in {
    
     
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
      
      val joinPreparation = new Join()
      
      joinPreparation.dfListIn = Seq(e1, e2)
        
      joinPreparation.param = new JoinParams("I",
                  Seq(
                      new ColumnClausePair("code", "code", "=") ),
                    Seq( 
                      new ColumnAlias("code", "ID", Some("columnOrigin")),
                      new ColumnAlias("val", "val", Some("columnOrigin")),
                      new ColumnAlias("code", "code2", Some("columnJoin")),
                      new ColumnAlias("val", "val2", Some("columnJoin"))
                      )
                  )
                  
      
      val w2 = joinPreparation.execute

      val res2 = Await.result( w2 ,
          Duration.apply(2, TimeUnit.MINUTES)) 

      res2 must beSome
      res2.get.count() === 12

    }
  
  }
   
}
