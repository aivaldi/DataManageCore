
package ar.com.ivalsoft.test.execution

import ar.com.ivalsoft.etl.information.SQLSourceInformation
import ar.com.ivalsoft.test.GeneralSpec
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import scala.concurrent.ExecutionContext

@RunWith(classOf[JUnitRunner])
class TestSourceInformationSpec extends GeneralSpec {

    
  implicit val sc =sconf.getOrCreate()

    implicit val ec = ExecutionContext.global
    
  "Sources Information" should {

    "SQL Source  Information" in {

      val sqlI = new SQLSourceInformation()

      sqlI.param = this.getTestDb1TableConnection()

      val info = sqlI.getInformation

      info must be_!=(null)

    }
  }
}
