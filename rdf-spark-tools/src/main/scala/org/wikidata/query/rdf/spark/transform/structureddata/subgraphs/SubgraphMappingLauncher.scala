package org.wikidata.query.rdf.spark.transform.structureddata.subgraphs

import scopt.OptionParser

case class SubgraphMappingParams(
                                  wikidataTriples: String = "", //in
                                  allSubgraphs: String = "", //out
                                  topSubgraphItems: String = "", //out
                                  topSubgraphTriples: String = "", //out
                                  minItems: Long = 10000
                                )

/**
 * Point of entry for wikidata subgraph mapping .
 * This job lists all subgraphs and maps items and triples to subgraphs in wikidata,
 * and saves parquet files in date format.
 *
 * Command line example:
 * spark2-submit --master yarn --driver-memory 2G --executor-memory 16G --executor-cores 8 \
 * --class org.wikidata.query.rdf.spark.transform.structureddata.subgraphs.SubgraphMappingLauncher \
 * --name subgraph-mapper-spark \
 * --queue root.default \
 * ~akhatun/rdf-spark-tools-0.3.42-SNAPSHOT-jar-with-dependencies.jar \
 * --wikidata-table discovery.wikibase_rdf/date=20220210/wiki=wikidata \
 * --all-subgraphs-table discovery.table_name/date=20220210/wiki=wikidata \
 * --top-subgraph-items-table discovery.table_name/date=20220210/wiki=wikidata \
 * --top-subgraph-triples-table discovery.table_name/date=20220210/wiki=wikidata \
 * --min-items 10000
 */
object SubgraphMappingLauncher {

  def main(args: Array[String]): Unit = {
    argParser.parse(args, SubgraphMappingParams()) match {

      case Some(params) => SubgraphMapper.extractAndSaveSubgraphMapping(
        params.wikidataTriples,
        params.minItems,
        params.allSubgraphs,
        params.topSubgraphItems,
        params.topSubgraphTriples
      )
      case _ => sys.exit(-1)

    }
  }

  def argParser: OptionParser[SubgraphMappingParams] = {
    new OptionParser[SubgraphMappingParams]("") {
      head("Subgraph Mapping Launcher")
      help("help") text "Prints this usage text"

      opt[String]("wikidata-table")
        .required()
        .valueName("<table-path>")
        .action { (x, p) => p.copy(wikidataTriples = x) }
        .text("Table holding Wikidata snapshots with partition specs")

      opt[String]("all-subgraphs-table")
        .required()
        .valueName("<table-path>")
        .action { (x, p) => p.copy(allSubgraphs = x) }
        .text("Output table holding list of subgraphs and item counts with partition specs")

      opt[String]("top-subgraph-items-table")
        .required()
        .valueName("<table-path>")
        .action { (x, p) => p.copy(topSubgraphItems = x) }
        .text("Output table holding items of top subgraphs (<item> P31 <subgraph>) with partition specs")

      opt[String]("top-subgraph-triples-table")
        .required()
        .valueName("<table-path>")
        .action { (x, p) => p.copy(topSubgraphTriples = x) }
        .text("Output table holding all triples of top subgraphs with partition specs")

      opt[Long]("min-items")
        .optional()
        .action { (x, p) => p.copy(minItems = x) }
        .text("Minimum number of items a subgraph should have to be considered as a top subgraph. Defaults to 10,000")
    }
  }

}
