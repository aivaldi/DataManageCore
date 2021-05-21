package ar.com.ivalsoft.spark.source.parser.ganalytics

import ar.com.ivalsoft.spark.source.parser.SourceParser
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

class GAnalyticsParser(parameter: GAnalyticsParams) extends SourceParser {

  override def toDataFrame(sc: SparkSession): Option[DataFrame] = {
    log.info("GAnalyticsParser creando RDD")

    val metrics = parameter.metrics.split(",").filter(_.nonEmpty).map(_.trim) ++ parameter.calculatedMetrics.split(",").filter(_.nonEmpty).map("calcMetric_" + _.trim)
    var (head, tail) =
      if (parameter.queryIndividualDays == "true") ("date", metrics)
      else (metrics.head, metrics.tail)
   

    val df =     
      if (parameter.calculatedMetrics.nonEmpty)
        sc.read
          .format("com.crealytics.google.analytics")
          .option("serviceAccountId", parameter.serviceAccountId)
          .option("keyFileLocation", parameter.credentialPath)
          .option("ids", parameter.ids)
          .option("startDate", parameter.startDate)
          .option("endDate", parameter.endDate)
          .option("queryIndividualDays", parameter.queryIndividualDays)
          .option("calculatedMetrics", parameter.calculatedMetrics)
          .load().select(head, tail:_*).coalesce(1)      
      else
        sc.read
          .format("com.crealytics.google.analytics")
          .option("serviceAccountId", parameter.serviceAccountId)
          .option("keyFileLocation", parameter.credentialPath)
          .option("ids", parameter.ids)
          .option("startDate", parameter.startDate)
          .option("endDate", parameter.endDate)
          .option("queryIndividualDays", parameter.queryIndividualDays)
          .load().select(head, tail:_*).coalesce(1)      
      
    log.info("GAnalyticsParser RDD Creado")

    Some(df)
  }

}