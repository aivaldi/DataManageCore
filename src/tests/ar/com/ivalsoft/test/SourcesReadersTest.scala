
package ar.com.ivalsoft.test

import org.specs2.mutable._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import java.sql.Time

import ar.com.ivalsoft.etl.executor.reader.SQLDataReader
import ar.com.ivalsoft.spark.source.persister.sql.SQLConnection
import ar.com.ivalsoft.spark.source.persister.SourcePersister
import ar.com.ivalsoft.spark.source.persister.sql.SQLPersister
import org.apache.spark.sql.SparkSession
import ar.com.ivalsoft.spark.source.persister.parquet.ParquetConnection
import ar.com.ivalsoft.spark.source.persister.parquet.ParquetPersister
import ar.com.ivalsoft.spark.source.parser.excel.ExcelParams
import ar.com.ivalsoft.etl.executor.reader.GenericDataReader
import ar.com.ivalsoft.spark.source.parser.csv.CSVFileParser
import ar.com.ivalsoft.spark.source.parser.excel.ExcelParser
import ar.com.ivalsoft.spark.source.persister.parquet.ParquetConnection
import ar.com.ivalsoft.spark.source.parser.json.JsonParams
import ar.com.ivalsoft.spark.source.parser.json.JsonParser
import ar.com.ivalsoft.spark.source.parser.gsheets.GSheetsParams
import ar.com.ivalsoft.spark.source.parser.gsheets.GSheetsParser
import ar.com.ivalsoft.spark.source.parser.ganalytics.GAnalyticsParams
import ar.com.ivalsoft.spark.source.parser.ganalytics.GAnalyticsParser
import ar.com.ivalsoft.spark.source.parser.gadwords.GAdWordsParams
import ar.com.ivalsoft.spark.source.parser.gadwords.GAdWordsParser
import ar.com.ivalsoft.spark.source.parser.parquet.ParquetOutParams
import org.scalatest.Ignore

@Ignore
@RunWith(classOf[JUnitRunner])
class ignoreMe extends Specification {

  val sconf = SparkSession.builder().master("local[2]").appName("test")
    .config("spark.driver.memory", "6g")
    .config("spark.executor.memory", "6g")
    .config("spark.cores.max", "12")
    .config("spark.deploy.defaultCores", "3")
    .config("spark.driver.maxResultSize", "0")
    .config("spark.default.parallelism", "10")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryoserializer.buffer", "1024k")
  sconf.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  implicit val sc = sconf.getOrCreate()

  implicit val ec = ExecutionContext.global

  "Sources Executor" should
    {

      /*  "SQL Source  save to SQL" in {
    
     
      val executor1 = new SQLDataReader
      
      executor1.param = new SQLParams(
      "desarrollo\\SQL2k8",
       "Visionaris_DW",
       "usContabilidad.FactContabilidad",
       "sa",
       "sa")
      
      val seq1 = Seq(executor1.execute)
      
      val w = Future.sequence(seq1 )
      
      val res = Await.result( w ,
          Duration.apply(2, TimeUnit.MINUTES)) 
            
      
      res.length must be_==(1)
      
      val dfo1 = res(0)
      
      dfo1 must beSome
      
      
      var param = new MSSQLParameters
      
      param.server = "pc010"
      param.dataBaseName = "Visionarisext"
      param.user = "sa"
      param.password = "sa"
      param.tableName = "tablaSQLTest"
      val p = new SourcePersister with  MSSQLPersister
      p.params = param
      
      p.saveData(dfo1)
      
      1===1
    }
   */
      /*  "SQL Source with custom columns  save to SQL" in {
    
     
      val executor1 = new SQLDataReader
      
      executor1.param = new SQLParams(
      "desarrollo\\SQL2k8",
       "Visionaris_DW",
       "vFactContabilidad22",
       "sa",
       "sa",None, Some(Seq("signo")) )
      
      val seq1 = Seq(executor1.execute)
      
      val w = Future.sequence(seq1 )
      
      val res = Await.result( w ,
          Duration.apply(2, TimeUnit.MINUTES)) 
            
      
      res.length must be_==(1)
      
      val dfo1 = res(0)
      
      dfo1 must beSome
      
      
      var param = new MSSQLParameters
      
      param.server = "pc010"
      param.dataBaseName = "Visionarisext"
      param.user = "sa"
      param.password = "sa"
      param.tableName = "tablaSQLTestWithQuery"
      val p = new SourcePersister with  MSSQLPersister
      p.params = param
      
      p.saveData(dfo1)
      
      1===1
    }
   */
      //    "CSV Source save to SQL" in {
      //    
      //     
      //      val executor1 = new CSVDataReader
      //      executor1.param = new CSVParams("c:\\csv\\dim_articulos.csv","true","true",";")
      //      val seq1 = Seq(executor1.execute)
      //      
      //      val w = Future.sequence(seq1 )
      //      
      //      val res = Await.result( w ,
      //          Duration.apply(2, TimeUnit.MINUTES)) 
      //            
      //      
      //      res.length must be_==(1)
      //      
      //      val dfo1 = res(0)
      //      
      //      dfo1 must beSome
      //      
      //      
      //      var param = new SQLParameters
      //      
      //      param.dataBaseType = "sql_server"
      //      param.server = "pc011"
      //      param.dataBaseName = "noBorrarPruebasLocal"
      //      param.user = "sa"
      //      param.password = "sa"
      //      param.tableName = "tablaCSVTest"
      //      val p = new SourcePersister with  SQLPersister
      //      p.params = param
      //      
      //      p.saveData(dfo1)
      //      
      //      1===1
      //    }

      /*"CSV Source save to PARQUET" in {
        val params = new CSVParams("resources/test.csv", "true", "true", ";")
        val parser = new CSVFileParser(params)
        val executor1 = new GenericDataReader[CSVFileParser]("", parser)
        val seq1 = Seq(executor1.execute)
        val w = Future.sequence(seq1)
        val res = Await.result(w, Duration.apply(2, TimeUnit.MINUTES))
        res.length must be_==(1)
        val dfo1 = res(0)
        dfo1 must beSome
        val param = new ParquetParams(folder = "resources/parquet/csv")
        val connection = new ParquetConnection(param)
        val p = new ParquetPersister(connection)
        p.saveData(dfo1)
        1 === 1
      }

      "Excel Source save to PARQUET" in {
        val params = new ExcelParams("resources/test.xls")
        val parser = new ExcelParser(params)
        val executor1 = new GenericDataReader("", parser)
        val seq1 = Seq(executor1.execute)
        val w = Future.sequence(seq1)
        val res = Await.result(w, Duration.apply(2, TimeUnit.MINUTES))
        res.length must be_==(1)
        val dfo1 = res(0)
        dfo1 must beSome
        val param = new ParquetParams(folder = "resources/parquet/xls")
        val connection = new ParquetConnection(param)
        val p = new ParquetPersister(connection)
        p.saveData(dfo1)
        1 === 1
      }

      "Json Source save to Parquet" in {
        val params = new JsonParams("file:///home/daniel/Projects/visionaris/trunk/DataSpaEngine/resources/test.json") // FIXME find a way to use a non local URI 
        val parser = new JsonParser(params)
        val executor = new GenericDataReader("", parser)
        val seq = Seq(executor.execute)
        val w = Future.sequence(seq)
        val res = Await.result(w, Duration.apply(2, TimeUnit.MINUTES))
        res.length must be_==(1)
        val dfo1 = res(0)
        dfo1 must beSome
        val param = new ParquetParams("resources/parquet/json")
        val connection = new ParquetConnection(param)
        val p = new ParquetPersister(connection)
        p.saveData(dfo1)
        1 === 1
      }*/

      "Google Sheet Source save to Parquet" in {
        val params = new GSheetsParams(
          serviceAccountId = "visionaris@adwordsintegration-178518.iam.gserviceaccount.com", // TODO replace this with a sheet of the same service account used for analytics (we have already shared our test sheet with the service account)
          credentialPath = "resources/AdWordsIntegration-9ef0d7389fd6.p12", // TODO replace this with the same p12 file used for analytics
          spreadSheetId = "19dw9Bnr8L1YzmQIBNpfHoLLD-dHbqzRL4BiASQi09sk",
          sheetId = "Sheet1")
        val parser = new GSheetsParser(params)
        val executor = new GenericDataReader("", parser)
        val seq = Seq(executor.execute)
        val w = Future.sequence(seq)
        val res = Await.result(w, Duration.apply(2, TimeUnit.MINUTES))
        res.length must be_==(1)
        val dfo1 = res(0)
        dfo1 must beSome
        val param = new ParquetOutParams("resources/parquet/gsheets")
        val connection = new ParquetConnection(param)
        val p = new ParquetPersister(connection)
        p.saveData(dfo1)
        1 === 1
      }
/*
      "Google Analytics Source save to Parquet" in {
        val params = new GAnalyticsParams(
          serviceAccountId = "visionaris@adwordsintegration-178518.iam.gserviceaccount.com",
          credentialPath = "resources/AdWordsIntegration-9ef0d7389fd6.p12",
          ids = "ga:17107333", // FIXME This is a tracking number, but we need an ids of the form ga:[0-9]+ (where the number comes from the analytics view ID, it can be retrieved from the view ID by using the analytics.management.profiles.list method)
          startDate = "7daysAgo",
          endDate = "yesterday",
          calculatedMetrics = "averageEngagement")
        val parser = new GAnalyticsParser(params)
        val executor = new GenericDataReader("", parser)
        val seq = Seq(executor.execute)
        val w = Future.sequence(seq)
        val res = Await.result(w, Duration.apply(2, TimeUnit.MINUTES))
        res.length must be_==(1)
        val dfo1 = res(0)
        dfo1 must beSome
        val param = new ParquetParams("resources/parquet/analytics")
        val connection = new ParquetConnection(param)
        val p = new ParquetPersister(connection)
        p.saveData(dfo1)
        1 === 1
      }
*/
      /*"Google AdWords Source save to Parquet" in {
        val params = new GAdWordsParams(
          clientId = "560722282758-efvrqsdj9t9i95u28safpj0nkep7eplh.apps.googleusercontent.com",
          clientSecret = "2Oa949LmpBr_l7uc6KW56T68",
          clientCustomerId = "1926416722",
          developerToken = "mD5AaojsxCf8dFNby7VCnQ", // FIXME this developer token is not approved (it fails at checking the alloted quota).
          refreshToken = "1/Od7ZiM7ro7ikybmES-uVjY8GUZNg51L2rX-9zctxZug",
          reportType = "SHOPPING_PERFORMANCE_REPORT")
        val parser = new GAdWordsParser(params)
        val executor = new GenericDataReader("", parser)
        val seq = Seq(executor.execute)
        val w = Future.sequence(seq)
        val res = Await.result(w, Duration.apply(2, TimeUnit.MINUTES))
        res.length must be_==(1)
        val dfo1 = res(0)
        dfo1 must beSome
        val param = new ParquetParams("resources/parquet/analytics")
        val connection = new ParquetConnection(param)
        val p = new ParquetPersister(connection)
        p.saveData(dfo1)
        1 === 1
      }*/

    }
}
