
package ar.com.ivalsoft.test.execution

import java.util.concurrent.TimeUnit

import ar.com.ivalsoft.etl.executor.reader.SQLDataReader
import ar.com.ivalsoft.etl.executor.writer.SQLDataWriter
import ar.com.ivalsoft.spark.source.parser.sql.SQLParams
import ar.com.ivalsoft.test.GeneralSpec
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.specs2.mutable._
import org.specs2.runner.JUnitRunner

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class TestExecutorsSpec extends GeneralSpec {

 implicit val sc = sconf.getOrCreate()
 implicit val ec = ExecutionContext.global

  "Reader" should {

    "Save" in {

      val executor1 = new SQLDataReader

      executor1.param = this.getTestDb1TableConnection()

      val f1 = executor1.execute

      val executor2 = new SQLDataWriter

      executor2.param = new SQLParams(
        "my_sql",
        "localhost",
        "testdb",
        "tablpa272015",
        "root",
        "abc123",
        "3306",
        false,
        None,
        None,
        "O")

      executor2.dfListIn = Seq(f1)

      val f3 = executor2.execute

      val res = Await.result(f3,
        Duration.apply(2, TimeUnit.MINUTES))

      println(executor2.executionInformation)

      1 === 1
    }
  }
}
