package ar.com.ivalsoft.spark.source.parser.gsheets

import ar.com.ivalsoft.etl.executor.params.Params

case class GSheetsParams(
  serviceAccountId: String,
  credentialPath: String,
  spreadSheetId: String,
  sheetId: String
) extends Params