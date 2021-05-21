package ar.com.ivalsoft.spark.source.persister.parquet

import java.util.Properties
import org.apache.spark.sql.SaveMode
import ar.com.ivalsoft.spark.source.persister.DefaultConnection
import org.apache.log4j.Logger
import ar.com.ivalsoft.spark.source.parser.parquet.ParquetOutParams


/**
 * @author aivaldi
 *
 * es un wrapper para la conexion a la base de datos
 *
 */
class ParquetConnection(val parquetParam: ParquetOutParams) extends DefaultConnection {

  val log = Logger.getLogger(getClass.getName)

//  private var _folder: String = ""
//  private var _saveMode: SaveMode = SaveMode.Overwrite
//
//  def folder: String = _folder;
//  def folder_=(value: String): Unit = _folder = value
//
//  def saveMode: SaveMode = _saveMode;
//  def saveMode_=(value: SaveMode): Unit = _saveMode = value

}