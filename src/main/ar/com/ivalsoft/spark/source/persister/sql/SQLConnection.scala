package ar.com.ivalsoft.spark.source.persister.sql

import java.util.Properties
import org.apache.spark.sql.SaveMode
import ar.com.ivalsoft.spark.source.persister.DefaultConnection
import org.apache.log4j.Logger
import ar.com.ivalsoft.spark.source.parser.sql.SQLParams

/**
 * @author aivaldi
 *
 * es un wrapper para la conexion a la base de datos
 *
 * MS no iria pero no quiero refactorizar
 *
 */
class SQLConnection(val param: SQLParams) extends DefaultConnection {

  val log = Logger.getLogger(getClass.getName)

  def getURL: String = {

    param.dataBaseType.toLowerCase() match {

      case "sql_server" =>
        var portL = if (!param.port.equals("")) s":$param.port" else ""
        var r = s"jdbc:sqlserver://${param.server}$portL" +
          s";databaseName=${param.dataBaseName};" +
          (if (param.integratedSecurity) s"integratedSecurity=true;" /*authenticationScheme=JavaKerberos*/
          else s"integratedSecurity=false;user=${param.user};password=${param.password};")
        log.debug(r)
        r

      case "my_sql" =>
        var r = s"jdbc:mysql://address=(protocol=tcp)(host=${param.server})(port=${param.port})(user=${param.user})(password=${param.password})/${param.dataBaseName}?zeroDateTimeBehavior=CONVERT_TO_NULL"
        log.debug(r)
        r

      case _ =>
        log.error(s"No source detected for databaseType:${param.dataBaseType}")
        throw new Exception(s"No source detected for databaseType:${param.dataBaseType}")
    }

  }

  def getProperties: Properties = {
    var properties = new java.util.Properties

    properties.setProperty("user", param.user)
    properties.setProperty("password", param.password)

    properties

  }

}