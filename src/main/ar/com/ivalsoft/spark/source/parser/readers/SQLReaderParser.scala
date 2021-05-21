package ar.com.ivalsoft.spark.source.parser.readers

import org.apache.log4j.Logger
import ar.com.ivalsoft.spark.source.parser.SourceParser
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import java.util.Properties
import org.apache.spark.sql.SQLContext
import ar.com.ivalsoft.etl.executor.EtlSqlContext
import org.apache.spark.sql.SparkSession

/**
 * @author aivaldi
 * implemento la lectura de un archivo
 */
trait SQLReaderParser extends SourceParser {
  
  type T = Option[DataFrame]
  
  override val log = Logger.getLogger(getClass.getName)

  var file: String = "";

  def getDataAccess(sc: SparkSession): Option[DataFrame] = {

    val sqlContext = sc

    import sqlContext.implicits._

    log.info("getDataAccess -- URL: " + this.getURL + " -- Table: " + getTable)

    val df = sqlContext.read.format("jdbc").options(Map("url" -> this.getURL,
      "dbtable" -> getTable))
      .load().toDF()

    Some(df);

  }

  /**
   * Este metodo sobreescribe el select par la tabla
   *
   * la forma de escribirlo debe ser (query) as t
   * "(Select *  from ["+ tableName +"] WHERE 1=1) as t";
   *
   */
  def getTable(): String = {
    "(Select " + _cols + " from " + tableName + " WHERE 1=1) as t";
  }

  private var _dataBaseType: String = ""
  private var _server: String = ""
  private var _dataBaseName: String = ""
  private var _tableName: String = ""
  private var _user: String = ""
  private var _password: String = ""
  private var _integratedSecurity: Boolean = false
  private var _saveMode: SaveMode = SaveMode.Overwrite
  private var _cols: String = "*"
  private var _port: String = "1433"

  def server: String = _server;
  def server_=(value: String): Unit = _server = value

  def dataBaseName: String = _dataBaseName;
  def dataBaseName_=(value: String): Unit = _dataBaseName = value

  def tableName: String = _tableName;
  def tableName_=(value: String): Unit = _tableName = value

  def user: String = _user;
  def user_=(value: String): Unit = _user = value

  def password: String = _password;
  def password_=(value: String): Unit = _password = value

  def saveMode: SaveMode = _saveMode;
  def saveMode_=(value: SaveMode): Unit = _saveMode = value

  def cols: String = _cols;
  def cols_=(value: String): Unit = _cols = value

  def dataBaseType: String = _dataBaseType;
  def dataBaseType_=(value: String): Unit = _dataBaseType = value

  def integratedSecurity: Boolean = _integratedSecurity
  def integratedSecurity_=(value: Boolean): Unit = _integratedSecurity = value

  def port: String = _port;
  def port_=(value: String): Unit = _port = value

  def getURL: String = {

    _dataBaseType match {

      case "sql_server" =>
        var portL = if (!port.equals("")) s":$port" else ""
        var r = s"jdbc:sqlserver://$server$portL" +
          s";databaseName=$dataBaseName;" + (if (this.integratedSecurity) s"integratedSecurity=true;" /*authenticationScheme=JavaKerberos*/
          else s"integratedSecurity=false;user=$user;password=$password;")
        log.info(r)
        r

      case "my_sql" =>
        var r = s"jdbc:mysql://address=(protocol=tcp)(host=$server)(port=$port)(user=$user)(password=$password)/$dataBaseName?zeroDateTimeBehavior=convertToNull"
        log.debug(r)
        r

        log.error(s"No source detected for databaseType:${_dataBaseType}")
        throw new Exception(s"No source detected for databaseType:${_dataBaseType}")
    }

  }

  def getProperties: Properties = {
    var properties = new java.util.Properties

    properties.setProperty("user", user)
    properties.setProperty("password", password)

    properties

  }

  def dataAccessType(): String = "SQL"

}