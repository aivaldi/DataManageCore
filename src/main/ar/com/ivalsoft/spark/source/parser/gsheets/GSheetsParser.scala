package ar.com.ivalsoft.spark.source.parser.gsheets

import ar.com.ivalsoft.spark.source.parser.SourceParser
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

class GSheetsParser(parameter: GSheetsParams) extends SourceParser {

  override def toDataFrame(sc: SparkSession): Option[DataFrame] = {
    log.info("GSheetsParser creando RDD")

    val df = sc.read.format("com.github.potix2.spark.google.spreadsheets")
      .option("serviceAccountId", parameter.serviceAccountId)
      .option("credentialPath", parameter.credentialPath)
      .load(parameter.spreadSheetId + "/" + parameter.sheetId)
      .coalesce(1)

    log.info("GSheetsParser RDD Creado")

    Some(df)
  }

}