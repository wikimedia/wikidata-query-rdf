package org.wikidata.query.rdf.spark.transform.structureddata.dumps

import scopt.OptionParser

/**
 * This job converts a wikidata/commons turtle dump (rdf triples formatted in turtle, usually used to
 * populate blazegraph), into either parquet, avro or nt triples.
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
 * --conf spark.yarn.maxAppAttempts=1 \
 * --conf spark.dynamicAllocation.maxExecutors=25 \
 * --class org.wikidata.query.rdf.spark.WikidataTurtleDumpConverter \
 * --name airflow-spark \
 * --queue root.default \
 * ~dcausse/rdf-spark-tools-0.3.42-SNAPSHOT-jar-with-dependencies.jar \
 * --input-path $DUMPS/all_ttl/20200817/wikidata-20200817-all-BETA.ttl.bz2,$DUMPS/lexemes_ttl/20200816/wikidata-20200816-lexemes-BETA.ttl.bz2 \
 * --output-table discovery.wikidata_rdf/date=20200817 \
 * --skolemize
 */
object WikibaseRDFDumpConverter {
  /**
   * Main method, parsing args and launching the conversion
   *
   * @param args the arguments to parse
   */
  def main(args: Array[String]): Unit = {
    parseParams(args) match {
      case Some(params) => TurtleImporter.importDump(params)
      case _ => sys.exit(-1)
    }
  }

  implicit val siteRead: scopt.Read[Site.Value] = scopt.Read.reads(Site.withName)

  /**
   * CLI Option Parser for job parameters (fill-in Params case class)
   */
  private def argsParser: OptionParser[Params] = {
    new OptionParser[Params]("") {
      head("Wikidata Turtle Dump Converter", "")
      help("help") text "Prints this usage text"

      opt[Seq[String]]('i', "input-path") required() valueName "<path1>,<path2>..." action { (x, paths) =>
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

      opt[Site.Value]('S', "site") optional() valueName "<site>" action { (x, p) =>
        p.copy(site = x)
      } text s"Site from which this dump is produced (${Site.values.mkString(",")})"

      opt[Int]('L', "prefix-header-lines") optional() valueName "<prefix-header-lines>" action { (x, p) =>
        p.copy(prefixHeaderLines = x)
      } text s"Number of lines to pre-fetch to get the list of RDF prefixes from the RDF dump header. Defaults to 1000"
    }
  }

  def parseParams(args: Array[String]): Option[Params] = argsParser.parse(args, Params()) match {
    case Some(params) =>
      if (params.outputTable.isEmpty == params.outputPath.isEmpty) {
        Console.err.print("Either --output-table or --output-path must be provided\n")
        None
      }
      Some(params)
    case _ => None
  }
}
