package ar.com.ivalsoft.spark.source.parser.gadwords

import ar.com.ivalsoft.spark.source.parser.SourceParser
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

class GAdWordsParser(parameter: GAdWordsParams) extends SourceParser {

  override def toDataFrame(sc: SparkSession): Option[DataFrame] = {
    log.info("GAdWordsParser creando RDD")

    val df = sc.read
      .format("com.crealytics.google.adwords")
      .option("clientId", parameter.clientId)
      .option("clientSecret", parameter.clientSecret)
      .option("clientCustomerId", parameter.clientCustomerId)
      .option("developerToken", parameter.developerToken)
      .option("refreshToken", parameter.refreshToken)
      .option("reportType", parameter.reportType)
      .option("during", parameter.during)
      .option("userAgent", parameter.userAgent)
      .load()

    log.info("GAdWordsParser RDD creado")

    Some(df)
  }

}