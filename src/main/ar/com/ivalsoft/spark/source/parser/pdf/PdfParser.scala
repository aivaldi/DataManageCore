package ar.com.ivalsoft.spark.source.parser.pdf

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.io.Text
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper
import scala.collection.JavaConverters
import java.sql.Date
import java.io.File
import java.text.NumberFormat
import ar.com.ivalsoft.spark.source.parser.SourceParser

class PdfParser(parameter: PdfParams) extends SourceParser {

  var lineDelta = 0

  def toDataFrame(sc: SparkSession): Option[DataFrame] = {

    val folder = parameter.path

    log.info(s"PdfParser creating DataFrame from $folder")

    val schema = List(
      StructField("Archivo", StringType, true),
      StructField("Fecha", DateType, true),
      StructField("CUIT", StringType, true),
      StructField("Tipo", StringType, true),
      StructField("Codigo", StringType, true),
      StructField("Importeneto", DoubleType, true),
      StructField("IVA21", DoubleType, true),
      StructField("IVA105", DoubleType, true),
      StructField("Importetotal", DoubleType, true),
      StructField("CUITcliente", StringType, true))

    val data = getListOfFiles(new File(folder), List("pdf")).map(getTextFromPdf).toSeq

    val df = sc.createDataFrame(sc.sparkContext.parallelize(data), StructType(schema))
    
    log.info("PdfParser DataFrame created")
    
    Some(df)
  }

  def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }

  def getTextFromPdf(file: File) = {
    log.info(s"Parsing ${file.getName}")
    val pdf = PDDocument.load(file)
    val lines = new PDFTextStripper().getText(pdf).split("\n")

    // Uncomment to see the whole contents of the file
    //log.info(new PDFTextStripper().getText(pdf))

    pdf.close

    Row(file.getName, getEmissionDate(lines), getProviderCUIT(lines), getDocumentType(lines), getOperationCode(lines), getNetAmount(lines), getIVA21(lines), getIVA10(lines), getTotalAmount(lines), getClientCUIT(lines))
  }

  def getEmissionDate(lines: Array[String]): java.sql.Date = {
    val format = new java.text.SimpleDateFormat("dd/MM/yyyy")
    // The emission date may come in line 11 or in line 12. If it comes in line 12 we have to separate it using whitespaces.
    val components = lines(11).trim.split(' ')(0).trim.split('/')

    // This constructor is deprecated. See if it is possible to change it in the future.
    val date = new Date(components(2).toInt - 1900, components(1).toInt - 1, components(0).toInt)
    if (components(2).toInt > 2018  || (components(2).toInt >= 2018 && components(1).toInt > 5) || (components(2).toInt >= 2018 && components(1).toInt >= 5 && components(0).toInt >= 31))
      lineDelta = 1
    else
      lineDelta = 0

    date
  }

  def getProviderCUIT(lines: Array[String]) = {
    // In the future we could check the CUIT for 11 digits
    // The other CUIT is in line 13
    Option(lines(12 + lineDelta).split(" ")(0)).getOrElse("NA").trim
  }

  def getClientCUIT(lines: Array[String]) = {
    Option(lines(13 + lineDelta).split(" ")(0)).getOrElse("NA").trim
  }

  def getDocumentType(lines: Array[String]): String = {
    var index = lines.indexWhere(_.startsWith("FACTURA"))
    if (index > 0)
      lines(index).trim
    else {
      index = lines.indexWhere(_.contains("NOTA DE CR"))
      if (index > 0)
        "NOTA DE CREDITO" + lines(index).trim.last
      else
        "NA"
    }
  }

  def getOperationCode(lines: Array[String]): String = {
    val index = lines.indexWhere(_.contains("Precio Unit"))

    /*
    Looks for 8 consecutive digits
    (?<!\d) - a negative lookbehind that will fail a match if there is a digit before 8 digits
    \d{8}+ 8 digits matched possessively (no backtracking allowed)
    (?!\d) - a negative lookahead that fails a match if there is a digit right after the 8 digits matched with the \d{8} subpattern
    */
    if (index < 0) "NA" else """(?<!\d)\d{8}+(?!\d)""".r.findFirstIn(lines(index + 1)).getOrElse("NA")
  }

  def getNetAmount(lines: Array[String]): Double = {
    val index = lines.indexWhere(_.contains("Importe Neto Gravado"))

    if (index < 0) 0.0 else lines(index).split(" ").last.trim.replaceAll(",", ".").toDouble
  }

  def getIVA21(lines: Array[String]): Double = {
    val index = lines.indexWhere(_.contains("IVA 21"))

    if (index < 0) 0.0 else lines(index).split(" ").last.trim.replaceAll(",", ".").toDouble
  }

  def getIVA10(lines: Array[String]): Double = {
    val index = lines.indexWhere(_.contains("IVA 10"))

    if (index < 0) 0.0 else lines(index).split(" ").last.trim.replaceAll(",", ".").toDouble
  }

  def getTotalAmount(lines: Array[String]): Double = {
    val index = lines.indexWhere(_.contains("Importe Total"))

    if (index < 0) 0.0 else lines(index).split(" ").last.trim.replaceAll(",", ".").toDouble
  }

}