package ar.com.ivalsoft.spark.source.parser.gadwords

import ar.com.ivalsoft.etl.executor.params.Params

case class GAdWordsParams(
  clientId: String,
  clientSecret: String,
  clientCustomerId: String,
  developerToken: String,
  refreshToken: String,
  reportType: String, // for a list of valid report types see https://developers.google.com/adwords/api/docs/appendix/reports#report-types
  during: String = "LAST_30_DAYS",
  userAgent: String = "Spark"
) extends Params