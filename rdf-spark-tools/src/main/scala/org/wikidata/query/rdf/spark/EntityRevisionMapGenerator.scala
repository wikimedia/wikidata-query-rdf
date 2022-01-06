package org.wikidata.query.rdf.spark

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.openrdf.model.Literal
import org.wikidata.query.rdf.common.uri.{FederatedUrisScheme, SchemaDotOrg, UrisScheme, UrisSchemeFactory}
import scopt.OptionParser

import java.net.URI

object EntityRevisionMapGenerator {
  val schema: StructType = StructType(Seq(
    StructField("entity", StringType, nullable = false),
    StructField("revision", LongType, nullable = false)
  ))

  /**
   * Class handling parsed parameters
   */
  case class Params(
                     table: String = "",
                     date: String = "",
                     outputPath: String = "",
                     hostname: String = "www.wikidata.org",
                     numPartitions: Int = 100,
                     urisScheme: String = "wikidata",
                     commonsConceptUri: Option[URI] = None,
                     wikidataConceptUri: Option[URI] = None
                   )

  /**
   * CLI Option Parser for job parameters (fill-in Params case class)
   */
  private val argsParser = new OptionParser[Params]("") {
    head("Wikidata Entity Revision map generator", "")
    help("help") text "Prints this usage text"

    opt[String]('t', "input-table") required() valueName  "<input-table>" action { (x, p) =>
      p.copy(table = x)
    } text "Table-partition holding the wikidata triples"

    opt[String]('u', "uris-scheme") optional() action { (x, p) =>
      p.copy(urisScheme = x)
    }

    opt[String]('h', "hostname") optional() valueName "<hostname>" action { (x, p) =>
      p.copy(hostname = x)
    } text "Hostname of the rdf data"

    opt[String]('o', "output-path") required() valueName "<output-path>" action { (x, p) =>
      p.copy(outputPath = if (x.endsWith("/")) x.dropRight(1) else x)
    } text "Path to output cvs file"

    opt[Int]('n', "num-partitions") optional() action { (x, p) =>
      p.copy(numPartitions = x)
    } text "Number of partitions to use (output files). Defaults to 100"

    opt[String]("commons-concept-uri") optional() action { (x, p) =>
      p.copy(commonsConceptUri = Some(URI.create(x)))
    } text "Overrides uri for commons"

    opt[String]("wikidata-concept-uri") optional() action { (x, p) =>
      p.copy(wikidataConceptUri = Some(URI.create(x)))
    } text "Overrides uri for wikidata"
  }

  def main(args: Array[String]): Unit = {
    argsParser.parse(args, Params()) match {
      case Some(params) =>

        val spark = SparkSession
          .builder()
          .appName("WikidataEntityRevisionMapGenerator")
          .getOrCreate()

        // Make spark read text with dedicated separator instead of end-of-line
        generateMap(spark, params.table, params.numPartitions,
          params.outputPath, urisScheme(params.urisScheme, params.hostname, params.commonsConceptUri, params.wikidataConceptUri))

      case None => sys.exit(1) // If args parsing fail (parser prints nice error)
    }
  }

  private def urisScheme(urisScheme: String, hostname: String, commonsConceptUri: Option[URI], wikidataConceptUri: Option[URI])(): UrisScheme = {
    urisScheme match {
      case "commons" => new FederatedUrisScheme(
        UrisSchemeFactory.forCommons(commonsConceptUri.getOrElse(UrisSchemeFactory.commonsUri(hostname))),
        UrisSchemeFactory.forWikidata(wikidataConceptUri.getOrElse(UrisSchemeFactory.wikidataUri(UrisSchemeFactory.WIKIDATA_HOSTNAME))))
      case "wikidata" =>
        UrisSchemeFactory.forWikidata(wikidataConceptUri.getOrElse(UrisSchemeFactory.wikidataUri(hostname)))
      case _ => throw new IllegalArgumentException(s"Unknown uris_scheme: $urisScheme")
    }
  }

  def generateMap(implicit spark: SparkSession,
                  tableAndPartitionSpec: String,
                  numPartitions: Int,
                  outputPath: String,
                  urisSchemeProvider: () => UrisScheme
                 ): Unit = {


    val versionPredicate = new StatementEncoder().encodeURI(SchemaDotOrg.VERSION)
    val df = SparkUtils.readTablePartition(tableAndPartitionSpec)
    val entityRevDf = spark.createDataFrame(
      df
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
