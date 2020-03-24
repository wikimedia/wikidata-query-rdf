package org.wikidata.query.rdf.spark

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import scopt.OptionParser


/**
 * This job converts a wikidata turtle dump (rdf triples formatted in turtle, usually used to
 * populate blazegraph), into either parquet or avro triples.
 *
 * It uses a special end-of-record separator to parallel-read the turtle text into entity-coherent portions.
 * Those portions are then streamed in parallel to an [[RdfChunkParser]] which generates RDF statements.
 * Those statements are finally converted to a Spark schema (entity, subject, predicate, object), and
 * written in the chosen format.
 *
 *
 * Command line example:
 * spark2-submit --master yarn --driver-memory 16G --executor-memory 32G --executor-cores 4 \
 *   --conf spark.dynamicAllocation.maxExecutors=32 \
 *   --conf spark.executor.memoryOverhead=8196 \
 *   --class org.wikidata.query.rdf.spark.WikidataTurtleDumpConverter \
 *   /home/joal/code/wikidata-query-rdf/rdf-spark-tools/target/rdf-spark-tools-0.3.14-SNAPSHOT.jar \
 *   -i /user/joal/wmf/data/raw/wikidata/dumps/all_ttl/20200120 \
 *   -o /user/joal/wmf/data/wmf/wikidata/triples/snapshot=20200120 \
 *   -f parquet \
 *   -n 1024
 */
object WikidataTurtleDumpConverter {
  private val ENTITY_HEADER = "data:"
  private val ENTITY_SEPARATOR = "\n" + ENTITY_HEADER

  /**
   * Class handling parsed parameters
   */
  case class Params(
                     inputPath: Seq[String] = Seq(),
                     outputPath: String = "",
                     outputFormat: String = "parquet",
                     numPartitions: Int = 512
                   )

  /**
   * CLI Option Parser for job parameters (fill-in Params case class)
   */
  private val argsParser = new OptionParser[Params]("") {
    head("Wikidata Turtle Dump Converter", "")
    help("help") text "Prints this usage text"

    opt[Seq[String]]('i', "input-path") required() valueName  "<path1>,<path2>..." action { (x, paths) =>
      paths.copy(inputPath = x map {
        path => if (path.endsWith("/")) path.dropRight(1) else path
      })
    } text "Paths to wikidata ttl dump to convert"

    opt[String]('o', "output-path") required() valueName "<path>" action { (x, p) =>
      p.copy(outputPath = if (x.endsWith("/")) x.dropRight(1) else x)
    } text "Path to output converted result."

    opt[String]('f', "output-format") optional() action { (x, p) =>
      p.copy(outputFormat = x)
    } validate { x =>
      if (!Seq("avro", "parquet").contains(x)) {
        failure("Invalid output format - can be avro or parquet")
      } else {
        success
      }
    } text "Output file format, avro or parquet. Defaults to parquet"

    opt[Int]('n', "num-partitions") optional() action { (x, p) =>
      p.copy(numPartitions = x)
    } text "Number of partitions to use (output files). Defaults to 512"
  }

  /**
   * Main method, parsing args and launching the conversion
   *
   * @param args the arguments to parse
   */
  def main(args: Array[String]): Unit = {
    argsParser.parse(args, Params()) match {
      case Some(params) =>

        val spark = SparkSession
          .builder()
          .appName("WikidataTurtleConverter")
          .getOrCreate()

        // Make spark read text with dedicated separator instead of end-of-line
        importDump(spark, params.inputPath, params.numPartitions, params.outputFormat, params.outputPath)

      case None => sys.exit(1) // If args parsing fail (parser prints nice error)
    }
  }

  def importDump(spark: SparkSession, inputPaths: Seq[String], numPartitions: Int, outputFormat: String, outputPath: String): Unit = {
    spark.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", ENTITY_SEPARATOR)

    val rdd = spark.sparkContext.union(inputPaths map {spark.sparkContext.textFile(_)})
      .flatMap(str => {
        // Filter out prefixes
        if (!str.startsWith("@prefix")) {
          // Parse entity turtle block (add entity header that have been removed by parsing)
          val is = new ByteArrayInputStream(s"$ENTITY_HEADER$str".getBytes(StandardCharsets.UTF_8))
          val statements = RdfChunkParser.forWikidata().parse(is)
          // Convert statements to rows
          val encoder = new StatementEncoder()
          statements.map(encoder.encode)
        } else {
          Seq.empty[Row]
        }
      })

    val df = spark.createDataFrame(rdd, StatementEncoder.schema)

    df.repartition(numPartitions).write
      .mode(SaveMode.Overwrite)
      .format(outputFormat)
      .save(outputPath)
  }
}
