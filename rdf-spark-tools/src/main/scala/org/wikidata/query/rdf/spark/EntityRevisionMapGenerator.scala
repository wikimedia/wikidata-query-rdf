package org.wikidata.query.rdf.spark

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.openrdf.model.Literal
import org.wikidata.query.rdf.common.uri.{SchemaDotOrg, UrisScheme, UrisSchemeFactory}
import scopt.OptionParser

object EntityRevisionMapGenerator {
  val schema: StructType = StructType(Seq(
    StructField("entity", StringType, nullable = false),
    StructField("revision", LongType, nullable = false)
  ))

  /**
   * Class handling parsed parameters
   */
  case class Params(
                     inputPath: String = "",
                     inputFormat: String = "parquet",
                     outputPath: String = "",
                     hostname: String = "www.wikidata.org",
                     numPartitions: Int = 100
                   )

  /**
   * CLI Option Parser for job parameters (fill-in Params case class)
   */
  private val argsParser = new OptionParser[Params]("") {
    head("Wikidata Entity Revision map generator", "")
    help("help") text "Prints this usage text"

    opt[String]('i', "input-path") required() valueName  "<path>" action { (x, p) =>
      val path = if (x.endsWith("/")) x.dropRight(1) else x
      p.copy(inputPath = path)
    } text "Path to wikidata graph dataframe"

    opt[String]('f', "input-format") optional() action { (x, p) =>
      p.copy(inputFormat = x)
    } validate { x =>
      if (!Seq("avro", "parquet").contains(x)) {
        failure("Invalid input format - can be avro or parquet")
      } else {
        success
      }
    } text "Output file format, avro or parquet. Defaults to parquet"

    opt[String]('h', "hostname") optional() valueName "<hostname>" action { (x, p) =>
      p.copy(hostname = x)
    } text "Hostname of the rdf data"

    opt[String]('o', "output-path") required() valueName "<path>" action { (x, p) =>
      p.copy(outputPath = if (x.endsWith("/")) x.dropRight(1) else x)
    } text "Path to output cvs file"

    opt[Int]('n', "num-partitions") optional() action { (x, p) =>
      p.copy(numPartitions = x)
    } text "Number of partitions to use (output files). Defaults to 100"
  }

  def main(args: Array[String]): Unit = {
    argsParser.parse(args, Params()) match {
      case Some(params) =>

        val spark = SparkSession
          .builder()
          .appName("WikidataEntityRevisionMapGenerator")
          .getOrCreate()

        // Make spark read text with dedicated separator instead of end-of-line
        generateMap(spark, params.inputPath, params.inputFormat, params.numPartitions,
          params.outputPath, () => UrisSchemeFactory.forHost(params.hostname))

      case None => sys.exit(1) // If args parsing fail (parser prints nice error)
    }
  }

  def generateMap(spark: SparkSession,
                  inputPath: String,
                  inputFormat: String,
                  numPartitions: Int,
                  outputPath: String,
                  urisSchemeProvider: () => UrisScheme
                 ): Unit = {


    val versionPredicate = new StatementEncoder().encodeURI(SchemaDotOrg.VERSION)
    val df = spark.read.format(inputFormat).load(inputPath)
    val entityRevDf = spark.createDataFrame(
      df
        .select("*")
        .filter(df("predicate") === versionPredicate)
        .rdd
        .map(r => {
          val encoder = new StatementEncoder()
          val statement = encoder.decode(r)
          val entity: String = urisSchemeProvider().entityURItoId(statement.getSubject.toString)
          val rev: Long = statement.getObject match {
            case e: Literal => e.longValue()
            case _: Any => -1 // We should probably fail?
          }
          Row.fromTuple(entity, rev)
        }), schema)
      // repartition so that the caller can control from the command line how many files will be generated
      // in the scenario these files need to be transferred out of hdfs
      .repartition(numPartitions)

    entityRevDf.write.format("csv")
      // force bzip2, flink is currently unable to detect&use the snappy codec
      .option("compression", "bzip2")
      .save(outputPath)
  }
}
