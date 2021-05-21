package ar.com.ivalsoft.spark.source.parser.ganalytics

import ar.com.ivalsoft.etl.executor.params.Params

case class GAnalyticsParams(
  serviceAccountId: String,
  credentialPath: String,
  ids: String,
  startDate: String,
  endDate: String,
  metrics: String,
  calculatedMetrics: String,
  queryIndividualDays: String = "false"
) extends Params