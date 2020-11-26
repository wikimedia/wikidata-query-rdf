package org.wikidata.query.rdf.spark

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import org.apache.hadoop.io.compress.{BZip2Codec, GzipCodec}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.openrdf.model.Statement

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
                   site: Site.Value = Site.wikidata
                 )

object TurtleImporter {
  private val WIKIDATA_ENTITY_HEADER = "data:"
  private val COMMONS_ENTITY_HEADER = "sdcdata:"

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
      .appName("WikibaseTurtleConverter")
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
    val entityHeader: String = params.site match{
      case Site.commons => COMMONS_ENTITY_HEADER
      case Site.wikidata => WIKIDATA_ENTITY_HEADER
    }
    val encoder = rowEncoder()
    // Make spark read text with dedicated separator instead of end-of-line
    val entitySeparator = "\n" + entityHeader
    spark.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", entitySeparator)
    val rdd: RDD[Row] = spark.sparkContext.union(params.inputPath map {spark.sparkContext.textFile(_)})
      .flatMap(str => {
        // Filter out prefixes
        if (!str.startsWith("@prefix")) {
          // Parse entity turtle block (add entity header that have been removed by parsing)
          val is = new ByteArrayInputStream(s"$entityHeader$str".getBytes(StandardCharsets.UTF_8))
          val chunkParser = RdfChunkParser.bySite(params.site, params.skolemizeBlankNodes)
          val statements = chunkParser.parse(is)
          // Convert statements to rows
          statements.map(encoder)
        } else {
          Seq.empty[Row]
        }
      })
    dataWriter(rdd)
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
