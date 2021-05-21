
package ar.com.ivalsoft.test.operators

import java.util.concurrent.TimeUnit

import ar.com.ivalsoft.etl.executor.order.params.OrderParams
import ar.com.ivalsoft.etl.executor.params.ColumnOrderPair
import ar.com.ivalsoft.etl.executor.reader.SQLDataReader
import ar.com.ivalsoft.test.GeneralSpec
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class TestOrderSpec extends GeneralSpec {

  implicit val sc = sconf.getOrCreate()
  implicit val ec = ExecutionContext.global

  "Order" should
    {

      "desc" in {

        val executor1 = new SQLDataReader

        executor1.param = this.getTestDb1TableConnection()

        val e1 = executor1.execute

        val res = Await.result(e1,
          Duration.apply(2, TimeUnit.MINUTES))

        res must beSome

        val order = new ar.com.ivalsoft.etl.executor.order.Order()

        order.dfListIn = Seq(e1)

        order.param = new OrderParams(

          Seq(
            new ColumnOrderPair("val", "desc")))

        val w2 = order.execute

        val res2 = Await.result(w2,
          Duration.apply(2, TimeUnit.MINUTES))

        res2 must beSome

        (res2.get.collect().toSeq.last)(2).toString() === 10.toString()

      }

      "Order 1 column asc" in {

        val executor1 = new SQLDataReader
        executor1.param = this.getTestDb1TableConnection()
        val e1 = executor1.execute

        val res = Await.result(e1,
          Duration.apply(2, TimeUnit.MINUTES))

        res must beSome

        val order = new ar.com.ivalsoft.etl.executor.order.Order()

        order.dfListIn = Seq(e1)

        order.param = new OrderParams(

          Seq(
            new ColumnOrderPair("val", "asc")))

        val w2 = order.execute

        val res2 = Await.result(w2,
          Duration.apply(2, TimeUnit.MINUTES))

        res2 must beSome


        (res2.get.collect().toSeq.last)(2).toString() === 100.toString()

      }

      "Order 2 columns asc and desc" in {

        val executor1 = new SQLDataReader
        executor1.param = this.getTestDb1TableConnection()

        val e1 = executor1.execute

        val res = Await.result(e1,
          Duration.apply(2, TimeUnit.MINUTES))

        res must beSome

        val order = new ar.com.ivalsoft.etl.executor.order.Order()

        order.dfListIn = Seq(e1)

        order.param = new OrderParams(

          Seq(
            new ColumnOrderPair("code", "desc"),
            new ColumnOrderPair("val", "asc")))

        val w2 = order.execute

        val res2 = Await.result(w2,
          Duration.apply(2, TimeUnit.MINUTES))

        res2 must beSome

        (res2.get.collect().toSeq.last)(1).toString() === 1.toString()
        (res2.get.collect().toSeq.last)(2).toString() === 12.toString()

      }

    }

}
