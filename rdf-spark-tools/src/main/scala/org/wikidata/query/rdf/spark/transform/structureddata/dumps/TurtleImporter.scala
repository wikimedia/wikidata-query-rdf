package org.wikidata.query.rdf.spark.transform.structureddata.dumps

import java.io.{ByteArrayInputStream, StringReader}
import java.nio.charset.StandardCharsets
import org.apache.hadoop.io.compress.{BZip2Codec, GzipCodec}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.SaveMode.Overwrite
import org.openrdf.model.Statement
import org.openrdf.rio.helpers.RDFHandlerBase
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory
import org.wikidata.query.rdf.spark.utils.SparkUtils
import org.wikidata.query.rdf.tool.rdf.RDFParserSuppliers

import scala.collection.mutable

object Site extends Enumeration {
  type Site = Value
  val wikidata, commons = Value
}

/**
 * Class handling parsed parameters
 */
case class Params(
                   inputPath: Seq[String] = Seq(),
                   outputTable: Option[String] = None,
                   outputPath: Option[String] = None,
                   outputFormat: String = "parquet",
                   numPartitions: Int = 512,
                   skolemizeBlankNodes: Boolean = false,
                   site: Site.Value = Site.wikidata,
                   prefixHeaderLines: Int = 1000
                 )

object TurtleImporter {
  def sparkSession: SparkSession = {
    SparkSession
      .builder()
      // required because spark would fail with:
      // Exception in thread "main" org.apache.spark.SparkException: Dynamic partition strict mode requires
      // at least one static partition column. To turn this off set // hive.exec.dynamic.partition.mode=nonstrict
      .config("hive.exec.dynamic.partition", value = true)
      .config("hive.exec.dynamic.partition.mode", "non-strict")
      // Allows overwriting the target partitions
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()
  }

  def importDump(params: Params, format: Option[String] = Some("hive")): Unit = {
    implicit val spark: SparkSession = sparkSession
    val dataWriter: RDD[Row] => Unit = params.outputTable match {
      case Some(table) =>
        getTableWriter(table, params.numPartitions, format)
      case None =>
        getDirectoryWriter(params.outputPath.get, params.outputFormat, params.numPartitions)
    }
    // do a quick first pass to fetch prefixes before setting "textinputformat.record.delimiter"
    val rdfPrefixes = fetchRdfPrefixes(params)

    val entityDataPrefix: String = params.site match {
      case Site.commons =>
        rdfPrefixWithMatchingURI(rdfPrefixes, UrisSchemeFactory.COMMONS.entityDataHttps())
      case Site.wikidata =>
        rdfPrefixWithMatchingURI(rdfPrefixes, UrisSchemeFactory.WIKIDATA.entityDataHttps())
    }
    val encoder = rowEncoder()
    // Make spark read text with dedicated separator instead of end-of-line
    val entitySeparator = f"\n$entityDataPrefix:"
    spark.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", entitySeparator)
    val rdd: RDD[Row] = spark.sparkContext.union(params.inputPath map {spark.sparkContext.textFile(_)})
      .flatMap(str => {
        // Filter out prefixes
        if (str.startsWith("@prefix")) {
          // parse the header that might contain some dump metadata
          val is = new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8))
          val chunkParser = RdfChunkParser.bySite(params.site, rdfPrefixes, params.skolemizeBlankNodes)
          val statements = chunkParser.parseHeader(is)
          // Convert statements to rows
          statements.map(encoder)
        } else {
          // Parse entity turtle block (add entity header that have been removed by parsing)
          val is = new ByteArrayInputStream(s"$entityDataPrefix:$str".getBytes(StandardCharsets.UTF_8))
          val chunkParser = RdfChunkParser.bySite(params.site, rdfPrefixes, params.skolemizeBlankNodes)
          val statements = chunkParser.parseEntityChunk(is)
          // Convert statements to rows
          statements.map(encoder)
        }
      })
      .distinct()
    dataWriter(rdd)
  }

  private def rdfPrefixWithMatchingURI(prefixes: Map[String, String], uri: String): String = {
    prefixes.filter {
      case (_, v) => v.equals(uri)
    }.keys.head
  }

  def fetchRdfPrefixes(params: Params)(implicit spark: SparkSession): Map[String, String] = {
    val reconstructedPrefixHeader = params.inputPath.flatMap { file =>
      val prefixLines = spark.sparkContext.textFile(file).take(params.prefixHeaderLines)
        .filter(_.startsWith("@prefix "))
        .toSeq
      if (prefixLines.isEmpty) {
        throw new IllegalStateException(s"No prefixes were extracted from the dump file: $file")
      }
      if (prefixLines.size >= params.prefixHeaderLines) {
        throw new IllegalStateException(s"Fetched ${params.prefixHeaderLines} RDF prefixes from the header of $file, " +
          "it is possible that more are present and you should consider increasing --prefix-header-lines")
      }
      prefixLines
    } mkString("\n")
    val prefixes = mutable.Map[String, String]()
    val handler = new RDFHandlerBase {
      override def handleNamespace(prefix: String, uri: String): Unit = prefixes.put(prefix, uri)
    }
    val reader = new StringReader(reconstructedPrefixHeader)
    RDFParserSuppliers.defaultRdfParser().get(handler).parse(reader, "unused")
    prefixes.toMap
  }

  def rowEncoder(): Statement => Row = {
    val encoder = new StatementEncoder()
    stmt: Statement => {
      Row.fromTuple(encoder.encode(stmt))
    }
  }

  def getTableWriter(table: String, partitions: Int, format: Option[String])(implicit spark: SparkSession): RDD[Row] => Unit = {
    rdd: RDD[Row] => {
      val df = spark.createDataFrame(rdd, StatementEncoder.baseSchema)
        .repartition(partitions)
      SparkUtils.insertIntoTablePartition(table, df, saveMode = Overwrite, format = format)
    }
  }

  def getDirectoryWriter(outputPath: String, outputFormat: String, partitions: Int)(implicit spark: SparkSession): RDD[Row] => Unit = {
    if (outputFormat.startsWith("nt.")){
      getTextFileDirectoryWriter(outputPath, outputFormat, partitions)
    } else {
      getFormatDirectoryWriter(outputPath, outputFormat, partitions)
    }
  }

  def getFormatDirectoryWriter(outputPath: String, outputFormat: String, partitions: Int)(implicit spark: SparkSession): RDD[Row] => Unit = {
    rdd: RDD[Row] => {
      spark.createDataFrame(rdd, StatementEncoder.baseSchema)
        .repartition(partitions)
        .write
        .mode(Overwrite)
        .format(outputFormat)
        .save(outputPath)
    }
  }

  def getTextFileDirectoryWriter(outputPath: String, outputFormat: String, partitions: Int)(implicit spark: SparkSession): RDD[Row] => Unit = {
    rdd: RDD[Row] => {
      val codec = if (outputFormat.endsWith(".gz")) classOf[GzipCodec] else classOf[BZip2Codec]
      spark.createDataFrame(rdd, StatementEncoder.baseSchema)
        .repartition(partitions)
        .map(r => s"${r.getAs("subject")} ${r.getAs("predicate")} ${r.getAs("object")} .")(Encoders.STRING)
        .rdd
        .saveAsTextFile(outputPath, codec)
    }
  }
}
