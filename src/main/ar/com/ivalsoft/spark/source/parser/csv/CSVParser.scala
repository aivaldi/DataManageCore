package ar.com.ivalsoft.spark.source.parser.csv

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Set
import org.apache.commons.io.FileUtils
import java.io.File

import ar.com.ivalsoft.spark.source.parser.SourceParser

/**
 * @author aivaldi
 *
 * parser para los csv
 *
 * implemento como otener el parser dado un archivo
 *
 */

class CSVFileParser(parameter: CSVInParams) extends SourceParser {

  def toDataFrame(sc: SparkSession): Option[DataFrame] = {
    log.info("CSVParser creando RDD")

    var csv = sc.read
      .format("csv")
      //.option("mode", "FAILFAST")
      .option("header", parameter.header)
      .option("inferSchema", parameter.inferSchema)
      .option("delimiter", parameter.delimiter)
      .option("quote", parameter.quote)
      .option("escape", parameter.escape)
      .option("encoding", parameter.charset)
      .option("nullValue", parameter.nullValue)

    if (parameter.dateFormat != None)
      csv = csv.option("dateFormat", parameter.dateFormat.get)

    var df = csv.load(parameter.file)

    // detects if there is a duplicated column name. If so it changes to a new column name.
    var s = Set[String]()
    var i = 2
    var columnsNamesChanged = false
    val names = df.columns.map {
      name =>
        var newName = name 
        if (s contains name.toLowerCase) {
          newName = name + i.toString
          i += 1
          columnsNamesChanged = true
        }

        s += newName.toLowerCase
        newName
    }

    // If a column name changed
    // it reopens the CSV with the header as data, it skips the header, changes the header and saves it again in 
    // a new directory. Then deletes the old directory and changes the name of the new temporary one.
    if (columnsNamesChanged) {
      log.info("Reading again without the header")

      csv = sc.read
      .format("csv")
      //.option("mode", "FAILFAST")
      .option("header", false)
      .option("inferSchema", parameter.inferSchema)
      .option("delimiter", parameter.delimiter)
      .option("quote", parameter.quote)
      .option("escape", parameter.escape)
      .option("encoding", parameter.charset)
      .option("nullValue", parameter.nullValue)

      if (parameter.dateFormat != None)
        csv = csv.option("dateFormat", parameter.dateFormat.get)

      var df2 = csv.load(parameter.file)

      val header = df2.first() 
      df2 = df2.filter(row => row != header)

      log.info("Saving file with changed column names")

      df2.toDF(names: _*).coalesce(1).write.format("csv")
      .option("header", "true")
      .option("delimiter", parameter.delimiter)
      .option("nullValue", parameter.nullValue)
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save(parameter.file + ".temp")

      FileUtils.deleteQuietly(new File(parameter.file))
      Thread.sleep(1000)
      FileUtils.moveDirectory(new File(parameter.file + ".temp"), new File(parameter.file))

      // Now it reads the directory
      csv = sc.read
      .format("csv")
      //.option("mode", "FAILFAST")
      .option("header", parameter.header)
      .option("inferSchema", parameter.inferSchema)
      .option("delimiter", parameter.delimiter)
      .option("quote", parameter.quote)
      .option("escape", parameter.escape)
      .option("encoding", parameter.charset)
      .option("nullValue", parameter.nullValue)

      if (parameter.dateFormat != None)
        csv = csv.option("dateFormat", parameter.dateFormat.get)

      df = csv.load(parameter.file)
    }
   
    log.info("CSVParser RDD Creado")
    
    Some(df)
  }

}
