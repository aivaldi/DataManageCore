package ar.com.ivalsoft.etl.workflow

import java.util.concurrent.TimeUnit
import scala.collection.Map
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import org.apache.spark.SparkContext
import ar.com.ivalsoft.spark.source.executor.ExecutionInformation
import ar.com.ivalsoft.etl.executor.Executor
import ar.com.ivalsoft.etl.executor.ExecutorIn
import ar.com.ivalsoft.etl.executor.factory.GenericFactory.{ buildGeneric, buildGenericInput }
import ar.com.ivalsoft.etl.executor.filter.Filter
import ar.com.ivalsoft.etl.executor.filter.params.FilterParams
import ar.com.ivalsoft.etl.executor.function.AccumulatorColumn
import ar.com.ivalsoft.etl.executor.function.AddRows
import ar.com.ivalsoft.etl.executor.function.CalculatedColumn
import ar.com.ivalsoft.etl.executor.function.FillGapsDateTime
import ar.com.ivalsoft.etl.executor.function.ProrationColumn
import ar.com.ivalsoft.etl.executor.function.params.AddRowParams
import ar.com.ivalsoft.etl.executor.function.params.CalculatedColumnParams
import ar.com.ivalsoft.etl.executor.function.params.FillGapDateTimeFunctionParams
import ar.com.ivalsoft.etl.executor.function.params.ProrationFunctionParams
import ar.com.ivalsoft.etl.executor.function.params.WindowFunctionParams
import ar.com.ivalsoft.etl.executor.group.Group
import ar.com.ivalsoft.etl.executor.group.params.GroupParams
import ar.com.ivalsoft.etl.executor.join.Join
import ar.com.ivalsoft.etl.executor.join.Union
import ar.com.ivalsoft.etl.executor.join.params.JoinParams
import ar.com.ivalsoft.etl.executor.join.params.UnionParams
import ar.com.ivalsoft.etl.executor.order.Order
import ar.com.ivalsoft.etl.executor.order.params.OrderParams
import ar.com.ivalsoft.etl.executor.preparation.ConvertPreparation
import ar.com.ivalsoft.etl.executor.function.IncrementalColumnsPreparation
import ar.com.ivalsoft.etl.executor.preparation.SelectionPreparation
import ar.com.ivalsoft.etl.executor.preparation.params.ConvertPreparationParams
import ar.com.ivalsoft.etl.executor.function.params.IncrementalPreparationParams
import ar.com.ivalsoft.etl.executor.preparation.params.SelectionPreparationParams
import ar.com.ivalsoft.spark.source.parser.pdf.PdfParams
import ar.com.ivalsoft.etl.executor.reader.SQLDataReader
import ar.com.ivalsoft.etl.executor.writer.SQLDataWriter
import ar.com.ivalsoft.etl.executor.function.DateTimeDetailColumn
import ar.com.ivalsoft.etl.executor.function.params.DateTimeDetailColumnParams
import com.google.common.collect.Tables.TransposeTable
import ar.com.ivalsoft.etl.executor.preparation.UnPivotPreparation
import ar.com.ivalsoft.etl.executor.preparation.params.UnPivotPreparationParams
import scala.collection.mutable.ArrayBuffer
import ar.com.ivalsoft.spark.source.executor.ExecutionInformation
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import ar.com.ivalsoft.spark.source.parser.parquet.ParquetOutParams
import ar.com.ivalsoft.spark.source.parser.parquet.ParquetInParams
import ar.com.ivalsoft.etl.executor.writer.parquet.ParquetDataWriter
import ar.com.ivalsoft.etl.executor.writer.csv.CSVDataWriter
import ar.com.ivalsoft.etl.executor.preparation.CleaningPreparation
import ar.com.ivalsoft.etl.executor.preparation.params.CleaningPreparationParams
import ar.com.ivalsoft.etl.executor.preparation.DynamicTablePreparation
import ar.com.ivalsoft.etl.executor.preparation.params.DynamicTableParams
import ar.com.ivalsoft.etl.executor.join.MultipleJoin
import ar.com.ivalsoft.etl.executor.join.params.MultipleJoinParams
import ar.com.ivalsoft.etl.executor.reader.GenericDataReader
import ar.com.ivalsoft.spark.source.parser.excel.ExcelParams
import ar.com.ivalsoft.spark.source.parser.excel.ExcelParser
import ar.com.ivalsoft.spark.source.parser.pdf.PdfParser
import ar.com.ivalsoft.spark.source.parser.csv.CSVFileParser
import ar.com.ivalsoft.spark.source.parser.parquet.ParquetParser
import ar.com.ivalsoft.spark.source.parser.sql.SQLGenericParams
import ar.com.ivalsoft.spark.source.parser.sql.SQLParams
import ar.com.ivalsoft.spark.source.parser.csv.CSVInParams
import ar.com.ivalsoft.spark.source.parser.csv.CSVOutParams
import ar.com.ivalsoft.spark.source.parser.json.JsonParams
import ar.com.ivalsoft.spark.source.parser.json.JsonParser
import ar.com.ivalsoft.spark.source.parser.mongodb.MongoDBParams
import ar.com.ivalsoft.spark.source.parser.mongodb.MongoDBParser
import ar.com.ivalsoft.spark.source.parser.gsheets.GSheetsParams
import ar.com.ivalsoft.spark.source.parser.gsheets.GSheetsParser
import ar.com.ivalsoft.spark.source.parser.ganalytics.GAnalyticsParams
import ar.com.ivalsoft.spark.source.parser.ganalytics.GAnalyticsParser
import ar.com.ivalsoft.spark.source.parser.gadwords.GAdWordsParams
import ar.com.ivalsoft.spark.source.parser.gadwords.GAdWordsParser
import org.apache.log4j.Logger

case class ExecutorWrapper(executor: Executor, executorLevel: Int, executorId: String)

/**
 * @author aivaldi
 * Contiene el esquema del workflow a ser ejecutado
 * Listado de los nodos
 * Listado de las conexiones por nodos
 */
class WorkflowSchema(nodes: Iterable[WorkflowNodes], links: Iterable[WorkflowLinks])(implicit val sc: SparkSession, implicit val ec: scala.concurrent.ExecutionContext) {

  def execute(): Iterable[ExecutionInformation] = {

    val seqNodes = nodes.map { x => getExecutor(x) }.toSeq

    val mapNodes = Map(seqNodes map { x => x.executorId -> x }: _*)

    links.toSeq.sortWith((a, b) => a.level < b.level).foreach { link =>
      val node: Option[ExecutorWrapper] = mapNodes.get(link.nodeId)
      if (node.isEmpty)
        throw new Exception(s"WorkflowSchema no node found with id ${link.nodeId}")

      if (node.get.executor.isInstanceOf[ar.com.ivalsoft.etl.executor.ExecutorIn]) {

        node.get.executor.asInstanceOf[ExecutorIn].dfListIn = link.in.map { idLinkIn =>

          val nodeIn: Option[ExecutorWrapper] = mapNodes.get(idLinkIn)
          if (nodeIn.isEmpty)
            throw new Exception(s"WorkflowSchema no nodeIn found with id ${idLinkIn}")

          nodeIn.get.executor.execute
        }
      }
    }

    val endNode = seqNodes.filter { x => x.executorLevel == 1000 }

    Logger.getLogger(getClass.getName).info("End nodes: " + endNode.map { _.executorId }.mkString(" "))

    if (endNode.isEmpty)
      throw new Exception("Not end node detected")

    val seqExecutors = Future.sequence(
      endNode.map { x => x.executor.execute } toSeq)

    val res = Await.result(seqExecutors,
      Duration.apply(600, TimeUnit.MINUTES))

    endNode.map { n => n.executor.executionInformation } toSeq

  }

  /**
   * Ejecuta hasta
   * Ejecuta cada una de las cajas indicadas, obteniendo el top 10 de cada una de ellas.
   */
  def executeUntil(): Map[String, ExecutionInformation] = {

    val seqNodes = nodes.map { x => getExecutor(x) }.toSeq

    val mapNodes = Map(seqNodes.map { x => x.executorId -> x }: _*)

    links.toSeq.sortWith((a, b) => a.level < b.level).foreach { link =>
      val node: Option[ExecutorWrapper] = mapNodes.get(link.nodeId)
      if (node.isEmpty)
        throw new Exception(s"WorkflowSchema no node found with id ${link.nodeId}")

      if (node.get.executor.isInstanceOf[ar.com.ivalsoft.etl.executor.ExecutorIn]) {

        node.get.executor.asInstanceOf[ExecutorIn].dfListIn = link.in.map { idLinkIn =>

          val nodeIn: Option[ExecutorWrapper] = mapNodes.get(idLinkIn)
          if (nodeIn.isEmpty)
            throw new Exception(s"WorkflowSchema no nodeIn found with id ${idLinkIn}")

          nodeIn.get.executor.execute
        }
      }
    }

    var ret: Map[String, ExecutionInformation] = Map.empty

    seqNodes.foreach {
      executor =>
        val res = Await.result(executor.executor.execute,
          Duration.apply(600, TimeUnit.MINUTES))

        ret += (executor.executorId -> getExecutionInformation(res.get))

    }

    ret

  }

  private def getExecutionInformation(df: DataFrame): ExecutionInformation = {

    val res = Some(df.head(10).map { x =>
      x.toSeq.map {
        case null => ""
        case y    => y.toString
      }
    } toSeq)

    new ExecutionInformation(None, Some(df.count()), res, Some(df.columns), None)

  }

  def getExecutor(node: WorkflowNodes): ExecutorWrapper = {
    val executor: Executor =
      node.nType match {
        case "SQLReader" =>
          if (node.params.isInstanceOf[SQLGenericParams])
            buildGeneric[SQLDataReader, SQLGenericParams](node)(() => new SQLDataReader())
          else
            buildGeneric[SQLDataReader, SQLParams](node)(() => new SQLDataReader())
        case "SQLWriter"             => buildGeneric[SQLDataWriter, SQLParams](node)(() => new SQLDataWriter())
        case "ParquetWriter"         => buildGeneric[ParquetDataWriter, ParquetOutParams](node)(() => new ParquetDataWriter())
        case "CSVWriter"             => buildGeneric[CSVDataWriter, CSVOutParams](node)(() => new CSVDataWriter())
        case "Selection"             => buildGeneric[SelectionPreparation, SelectionPreparationParams](node)(() => new SelectionPreparation())
        case "Convert"               => buildGeneric[ConvertPreparation, ConvertPreparationParams](node)(() => new ConvertPreparation())
        case "Join"                  => buildGeneric[Join, JoinParams](node)(() => new Join())
        case "Union"                 => buildGeneric[Union, UnionParams](node)(() => new Union())
        case "Filter"                => buildGeneric[Filter, FilterParams](node)(() => new Filter())
        case "Order"                 => buildGeneric[Order, OrderParams](node)(() => new Order())
        case "Group"                 => buildGeneric[Group, GroupParams](node)(() => new Group())
        case "CalculatedColumn"      => buildGeneric[CalculatedColumn, CalculatedColumnParams](node)(() => new CalculatedColumn())
        case "GenerateIdColumn"      => buildGeneric[IncrementalColumnsPreparation, IncrementalPreparationParams](node)(() => new IncrementalColumnsPreparation())
        case "AccumulatedColumn"     => buildGeneric[AccumulatorColumn, WindowFunctionParams](node)(() => new AccumulatorColumn())
        case "ProrationColumn"       => buildGeneric[ProrationColumn, ProrationFunctionParams](node)(() => new ProrationColumn())
        case "FillGapDateTimeColumn" => buildGeneric[FillGapsDateTime, FillGapDateTimeFunctionParams](node)(() => new FillGapsDateTime())
        case "AddRows"               => buildGeneric[AddRows, AddRowParams](node)(() => new AddRows())
        case "AddPeriodColumns"      => buildGeneric[DateTimeDetailColumn, DateTimeDetailColumnParams](node)(() => new DateTimeDetailColumn())
        case "UnPivoteTable"         => buildGeneric[UnPivotPreparation, UnPivotPreparationParams](node)(() => new UnPivotPreparation())
        case "Clean"                 => buildGeneric[CleaningPreparation, CleaningPreparationParams](node)(() => new CleaningPreparation())
        case "DynamicTable"          => buildGeneric[DynamicTablePreparation, DynamicTableParams](node)(() => new DynamicTablePreparation())
        case "MultipleJoin"          => buildGeneric[MultipleJoin, MultipleJoinParams](node)(() => new MultipleJoin())
        case "CSVReader"             => buildGenericInput[CSVInParams, CSVFileParser](node)(new CSVFileParser(_))
        case "MongoDBReader"         => buildGenericInput[MongoDBParams, MongoDBParser](node)(new MongoDBParser(_))
        case "ExcelReader"           => buildGenericInput[ExcelParams, ExcelParser](node)(new ExcelParser(_))
        case "ParquetReader"         => buildGenericInput[ParquetInParams, ParquetParser](node)(new ParquetParser(_))
        case "PdfReader"             => buildGenericInput[PdfParams, PdfParser](node)(new PdfParser(_))
        case "JsonReader"            => buildGenericInput[JsonParams, JsonParser](node)(new JsonParser(_))
        case "GSheetsReader"         => buildGenericInput[GSheetsParams, GSheetsParser](node)(new GSheetsParser(_))
        case "GAnalyticsReader"      => buildGenericInput[GAnalyticsParams, GAnalyticsParser](node)(new GAnalyticsParser(_))
        case "GAdWordsReader"        => buildGenericInput[GAdWordsParams, GAdWordsParser](node)(new GAdWordsParser(_))
        case _                       => throw new IllegalArgumentException(s"executor type not found ${node.nType}")
      }

    new ExecutorWrapper(
      executor,
      node.level,
      node.id)

  }

}