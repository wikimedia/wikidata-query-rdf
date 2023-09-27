package org.wikidata.query.rdf.spark.transform.queries.sparql

import scopt.OptionParser

case class Params(
                   inputTable: String = "",
                   outputTable: String = "",
                   inputPartitions: Int = 1,
                   outputPartitions: Int = 1
                 )
/**
 * Point of entry for sparql query extraction.
 * This job processes raw SPARQL into analyzable format in Spark schema from [[QueryInfo]]
 * and saves parquet files in hourly format.
 *
 * Command line example:
 * spark2-submit --master yarn --driver-memory 2G --executor-memory 16G --executor-cores 8 \
 *   --class org.wikidata.query.rdf.spark.analysis.QueryExtractor \
 *   --name sparql-extractor-spark \
 *   --queue root.default \
 *   ~akhatun/rdf-spark-tools-0.3.42-SNAPSHOT-jar-with-dependencies.jar \
 *   --input-table event.wdqs_external_sparql_query/year=2021/month=5/day=1/hour=1 \
 *   --output-table discovery.processed_wdqs_external_sparql_query/year=2021/month=5/day=1/hour=1/wiki=wikidata
 */
object QueryExtractor {

  def main(args: Array[String]): Unit = {
    argParser.parse(args, Params()) match {

      case Some(params) => QueriesProcessor.extractAndSaveQuery(params)
      case _ => sys.exit(-1)

    }
  }

  def argParser: OptionParser[Params] = {
    new OptionParser[Params]("") {
      head("Sparql Query Extractor")
      help("help") text "Prints this usage text"

      opt[String]('i', "input-table") required() valueName "<input-table>" action { (x, p) =>
        p.copy(inputTable = x)
      } text "Table holding the SPARQL queries with partition specs"

      opt[String]('o', "output-table") required() valueName "<output-table>" action { (x, p) =>
        p.copy(outputTable = x)
      } text "Table to output processed SPARQL queries with partition specs"

      opt[Int]('p', "num-partitions") optional() action { (x, p) =>
        p.copy(inputPartitions = x)
      } text "BC option for --input-partitions"

      opt[Int]("input-partitions") optional() action { (x, p) =>
        p.copy(inputPartitions = x)
      } text "Number of partitions to process the data with. Defaults to 1"

      opt[Int]("output-partitions") optional() action { (x, p) =>
        p.copy(outputPartitions = x)
      } text "Number of output files to write. Defaults to 1"
    }
  }

}
