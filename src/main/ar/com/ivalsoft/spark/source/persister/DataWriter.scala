package ar.com.ivalsoft.spark.source.persister

import org.apache.spark.sql.SaveMode

trait DataWriter {

  def toSaveMode(mode: String): SaveMode = {
    mode match {
      case "A" => SaveMode.Append
      case "O" => SaveMode.Overwrite
      case "I" => SaveMode.Ignore
      case "E" => SaveMode.ErrorIfExists
    }

  }

}