package ar.com.ivalsoft.test

import ar.com.ivalsoft.spark.source.parser.sql.SQLParams
import org.apache.spark.sql.SparkSession
import org.specs2.mutable.Specification

abstract class GeneralSpec extends Specification{
  val sconf =   SparkSession.builder().master("local[2]").appName("test")
    .config("spark.driver.memory", "6g")
    .config("spark.executor.memory", "6g")
    .config("spark.cores.max", "12")
    .config("spark.deploy.defaultCores", "3")
    .config("spark.driver.maxResultSize","0")
    .config("spark.default.parallelism","10")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryoserializer.buffer","1024k")
  sconf.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  def getTestDb2TableConnection () = {
    new SQLParams(
      "my_sql",
      "127.0.0.1",
      "testdb",
      "test2",
      "root",
      "abc123", "3306")
  }
  def getTestDb1TableConnection () = {
    new SQLParams(
      "my_sql",
      "127.0.0.1",
      "testdb",
      "test1",
      "root",
      "abc123", "3306")
  }
  def getTestDb3TableConnection () = {
    new SQLParams(
      "my_sql",
      "127.0.0.1",
      "testdb",
      "test3",
      "root",
      "abc123", "3306")
  }
}
