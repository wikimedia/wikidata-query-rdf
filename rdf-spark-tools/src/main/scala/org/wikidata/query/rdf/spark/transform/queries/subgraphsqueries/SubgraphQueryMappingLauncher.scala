package org.wikidata.query.rdf.spark.transform.queries.subgraphsqueries

import scopt.OptionParser

case class SubgraphQueryMappingParams(
                                       wikidataTriples: String = "",
                                       processedQuery: String = "",
                                       topSubgraphItems: String = "",
                                       topSubgraphTriples: String = "",
                                       subgraphQItemsMatchInQuery: String = "", //out
                                       subgraphPredicatesMatchInQuery: String = "", //out
                                       subgraphUrisMatchInQuery: String = "", //out
                                       queryMapping: String = "", //out
                                       filteringLimit: Double = 99
                                     )

/**
 * Point of entry for wikidata subgraph query mapping .
 * This job maps all SPARQL queries in WDQS to one or more subgraphs they access
 * and saves parquet files in daily format.
 *
 * Command line example:
 * spark2-submit --master yarn --driver-memory 2G --executor-memory 16G --executor-cores 8 \
 * --class org.wikidata.query.rdf.spark.transform.queries.subgraphsqueries.SubgraphQueryMappingLauncher \
 * --name subgraph-query-mapper-spark \
 * --queue root.default \
 * ~akhatun/rdf-spark-tools-0.3.42-SNAPSHOT-jar-with-dependencies.jar \
 * --wikidata-table discovery.wikibase_rdf/date=20220210/wiki=wikidata \
 * --processed-query-table discovery.processed_external_sparql_query/year=2021/month=5/day=1/wiki=wikidata \
 * --top-subgraph-items-table discovery.table_name/date=20220210/wiki=wikidata \
 * --top-subgraph-triples-table discovery.table_name/date=20220210/wiki=wikidata \
 * --subgraph-qitem-match-query-table table_name/year=2021/month=5/day=1/wiki=wikidata \
 * --subgraph-predicate-match-query-table table_name/year=2021/month=5/day=1/wiki=wikidata \
 * --subgraph-uri-match-query-table table_name/year=2021/month=5/day=1/wiki=wikidata \
 * --subgraph-query-mapping-table table_name/year=2021/month=5/day=1/wiki=wikidata \
 * --filtering-limit 99
 */
object SubgraphQueryMappingLauncher {

  def main(args: Array[String]): Unit = {
    argParser.parse(args, SubgraphQueryMappingParams()) match {

      case Some(params) => SubgraphQueryMapper.extractAndSaveSubgraphQueryMapping(
        params.wikidataTriples,
        params.processedQuery,
        params.topSubgraphTriples,
        params.topSubgraphItems,
        params.filteringLimit,
        params.subgraphQItemsMatchInQuery,
        params.subgraphPredicatesMatchInQuery,
        params.subgraphUrisMatchInQuery,
        params.queryMapping
      )
      case _ => sys.exit(-1)

    }
  }

  // method has several input arguments to define
  // scalastyle:off method.length
  def argParser: OptionParser[SubgraphQueryMappingParams] = {
    new OptionParser[SubgraphQueryMappingParams]("") {
      head("Subgraph Query Mapping Launcher")
      help("help") text "Prints this usage text"

      opt[String]("wikidata-table")
        .required()
        .valueName("<table-path>")
        .action { (x, p) => p.copy(wikidataTriples = x) }
        .text("Table holding Wikidata snapshots with partition specs")

      opt[String]("processed-query-table")
        .required()
        .valueName("<table-path>")
        .action { (x, p) => p.copy(processedQuery = x) }
        .text("Table holding the parsed SPARQL queries with partition specs")

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

      opt[String]("subgraph-qitem-match-query-table")
        .required()
        .valueName("<table-path>")
        .action { (x, p) => p.copy(subgraphQItemsMatchInQuery = x) }
        .text("Output table holding query id and qitem match with subgraphs with partition specs")

      opt[String]("subgraph-predicate-match-query-table")
        .required()
        .valueName("<table-path>")
        .action { (x, p) => p.copy(subgraphPredicatesMatchInQuery = x) }
        .text("Output table holding query id and predicate match with subgraphs with partition specs")

      opt[String]("subgraph-uri-match-query-table")
        .required()
        .valueName("<table-path>")
        .action { (x, p) => p.copy(subgraphUrisMatchInQuery = x) }
        .text("Output table holding query id and uris match with subgraphs with partition specs")

      opt[String]("subgraph-query-mapping-table")
        .required()
        .valueName("<table-path>")
        .action { (x, p) => p.copy(queryMapping = x) }
        .text("Output table holding mapping of query id and subgraph with partition specs")

      opt[Int]("filtering-limit")
        .required()
        .valueName("<number>")
        .action { (x, p) => p.copy(filteringLimit = x) }
        .text("The percent limit used to associate an item, uri, or property to a particular subgraph. Value is in range [0,100]")
        .validate(x =>
          if (x >= 0) {
            success
          }
          else {
            failure("Option --filtering-limit must be >=0")
          })
        .validate(x =>
          if (x <= 100) {
            success
          }
          else {
            failure("Option --filtering-limit must be <=100")
          })
    }
  }
  // scalastyle:on method.length

}
