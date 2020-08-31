package org.wikidata.query.rdf.spark

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.SaveMode.Overwrite
import org.openrdf.model.Statement
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
 * DUMP=hdfs://analytics-hadoop/wmf/data/raw/wikidata/dumps
 * spark2-submit --master yarn --driver-memory 2G --executor-memory 16G --executor-cores 8 \
 *   --conf spark.yarn.maxAppAttempts=1 \
 *   --conf spark.dynamicAllocation.maxExecutors=25 \
 *   --class org.wikidata.query.rdf.spark.WikidataTurtleDumpConverter \
 *   --name airflow-spark \
 *   --queue root.default \
 *   ~dcausse/rdf-spark-tools-0.3.42-SNAPSHOT-jar-with-dependencies.jar \
 *   --input-path $DUMPS/all_ttl/20200817/wikidata-20200817-all-BETA.ttl.bz2,$DUMPS/lexemes_ttl/20200816/wikidata-20200816-lexemes-BETA.ttl.bz2 \
 *   --output-table discovery.wikidata_rdf/date=20200817 \
 *   --skolemize
 */
object WikidataTurtleDumpConverter {
  private val ENTITY_HEADER = "data:"
  private val ENTITY_SEPARATOR = "\n" + ENTITY_HEADER

  /**
   * Class handling parsed parameters
   */
  case class Params(
                     inputPath: Seq[String] = Seq(),
                     outputTable: Option[String] = None,
                     outputPath: Option[String] = None,
                     outputFormat: String = "parquet",
                     numPartitions: Int = 512,
                     skolemizeBlankNodes: Boolean = false
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

    opt[String]('t', "output-table") optional() valueName "<output-table>" action { (x, p) =>
      p.copy(outputTable = Some(x))
    } text "Table to output converted result."

    opt[String]('o', "output-path") optional() valueName "<output-path>" action { (x, p) =>
      p.copy(outputPath = Some(x))
    } text "Path to output converted result."

    opt[String]('f', "output-format") optional() valueName "<output-format>" action { (x, p) =>
      p.copy(outputFormat = x)
    } text "Format to use when storing results using --output-path."

    opt[Int]('n', "num-partitions") optional() action { (x, p) =>
      p.copy(numPartitions = x)
    } text "Number of partitions to use (output files). Defaults to 512"

    opt[Unit]('s', "skolemize") action { (_, p) =>
      p.copy(skolemizeBlankNodes = true)
    } text "Skolemize blank nodes"
  }

  /**
   * Main method, parsing args and launching the conversion
   *
   * @param args the arguments to parse
   */
  def main(args: Array[String]): Unit = {
    argsParser.parse(args, Params()) match {
      case Some(params) =>
        if (params.outputTable.isEmpty == params.outputPath.isEmpty) {
          Console.err.print("Either --output-table or --output-path must be provided\n")
          sys.exit(1)
        }
        implicit val spark: SparkSession = SparkSession
          .builder()
          // required because spark would fail with:
          // Exception in thread "main" org.apache.spark.SparkException: Dynamic partition strict mode requires
          // at least one static partition column. To turn this off set // hive.exec.dynamic.partition.mode=nonstrict
          .config("hive.exec.dynamic.partition", value = true)
          .config("hive.exec.dynamic.partition.mode", "non-strict")
          // Allows overwriting the target partitions
          .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
          .appName("WikidataTurtleConverter")
          .getOrCreate()

        val dataWriter: RDD[Row] => Unit = params.outputTable match {
          case Some(table) =>
            getTableWriter(table, params.numPartitions, Some("hive"))
          case None =>
            getDirectoryWriter(params.outputPath.get, params.outputFormat, params.numPartitions)
        }
        importDump(params.inputPath, params.skolemizeBlankNodes, rowEncoder(), dataWriter)

      case None => sys.exit(1) // If args parsing fail (parser prints nice error)
    }
  }

  def importDump(inputPaths: Seq[String], skolemizeBlankNodes: Boolean, statementEncoder: Statement => Row,
                 rddWriter: RDD[Row] => Unit)(implicit spark: SparkSession): Unit = {
    // Make spark read text with dedicated separator instead of end-of-line
    spark.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", ENTITY_SEPARATOR)
    val rdd: RDD[Row] = spark.sparkContext.union(inputPaths map {spark.sparkContext.textFile(_)})
      .flatMap(str => {
        // Filter out prefixes
        if (!str.startsWith("@prefix")) {
          // Parse entity turtle block (add entity header that have been removed by parsing)
          val is = new ByteArrayInputStream(s"$ENTITY_HEADER$str".getBytes(StandardCharsets.UTF_8))
          val statements = RdfChunkParser.forWikidata(skolemizeBlankNodes).parse(is)
          // Convert statements to rows
          statements.map(statementEncoder)
        } else {
          Seq.empty[Row]
        }
      })
    rddWriter(rdd)
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
    rdd: RDD[Row] => {
      spark.createDataFrame(rdd, StatementEncoder.baseSchema)
        .repartition(partitions)
        .write
        .mode(Overwrite)
        .format(outputFormat)
        .save(outputPath)
    }
  }
}
