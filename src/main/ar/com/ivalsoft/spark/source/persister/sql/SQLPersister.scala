package ar.com.ivalsoft.spark.source.persister.sql

import java.sql.DriverManager

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import ar.com.ivalsoft.spark.source.executor.ExecutionInformation
import ar.com.ivalsoft.spark.source.persister.SourcePersister
import ar.com.ivalsoft.spark.source.persister.DataWriter

/**
 * @author aivaldi
 * Persiste los datos en un SQL
 */
class SQLPersister(val sqlConnection: SQLConnection) extends SourcePersister with DataWriter {

  private val log = Logger.getLogger(getClass.getName)

  def saveData(dataframe: Option[DataFrame])(implicit sc: SparkSession): ExecutionInformation = {

    if (dataframe.isEmpty) {
      log.error("No se encontro el data frame para persistir en MSSQL")
      return ExecutionInformation(None, None, None,None, Some("DATA_FRAME_NOT_FOUND"))
    }

    //Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance

    val df = dataframe.get

    val param = sqlConnection.param

    if (param.deleteValues) {
      val conn = DriverManager.getConnection(sqlConnection.getURL)
      val stm = conn.createStatement()
      stm.executeUpdate(s"If object_id('${param.tableName}','U') is not null TRUNCATE TABLE ${param.tableName}")
    }

    val sqlContext = (sc)

    //pongo todas las columnas como not null por ahora
    val dfAllowNulls = setNullableStateForAllColumns(df, true)

    dfAllowNulls.write.mode(toSaveMode(param.saveMode)).jdbc(sqlConnection.getURL, param.tableName, sqlConnection.getProperties)

    var res = Some(dfAllowNulls.head(10).map { x =>
      x.toSeq.map {
        case null => ""
        case y    => y.toString
      }
    } toSeq)

    new ExecutionInformation(Some(sqlConnection), Some(df.count()), res, Some(dfAllowNulls.columns) , None)
  }

  def setNullableStateForAllColumns(df: DataFrame, nullable: Boolean): DataFrame = {
    val schema = df.schema
    val newSchema = StructType(schema.map {
      case StructField(c, t, _, m) ⇒ StructField(c, t, nullable = nullable, m)
    })

    df.sqlContext.createDataFrame(df.rdd, newSchema)
  }

}