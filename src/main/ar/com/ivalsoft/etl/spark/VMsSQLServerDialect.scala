package ar.com.ivalsoft.etl.spark


import org.apache.spark.sql.types._
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.jdbc.JdbcType

/**
 * @author aivaldi
 */
object VMsSQLServerDialect extends JdbcDialect{
  
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:sqlserver")

  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (typeName.contains("datetimeoffset")) {
      // String is recommend by Microsoft SQL Server for datetimeoffset types in non-MS clients
      Option(StringType)
    } else {
      None
    }
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case TimestampType => Some(JdbcType("DATETIME", java.sql.Types.TIMESTAMP))
    case StringType => Some(JdbcType("VARCHAR(MAX)", java.sql.Types.VARCHAR))
    case BooleanType => Some(JdbcType("bit", java.sql.Types.BIT))
    case _ => None
  }
  
  
  
}