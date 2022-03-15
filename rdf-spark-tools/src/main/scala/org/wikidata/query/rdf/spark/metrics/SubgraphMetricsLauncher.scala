package org.wikidata.query.rdf.spark.metrics

import org.wikidata.query.rdf.spark.metrics.queries.subgraphpairs.SubgraphPairQueryMetricsExtractor.extractAndSaveSubgraphPairQueryMetrics
import org.wikidata.query.rdf.spark.metrics.queries.subgraphs.QueryMetricsExtractor.extractAndSaveQueryMetrics
import org.wikidata.query.rdf.spark.metrics.subgraphs.detailed.DetailedSubgraphMetricsExtractor.extractAndSaveDetailedSubgraphMetrics
import org.wikidata.query.rdf.spark.metrics.subgraphs.general.GeneralSubgraphMetricsExtractor.extractAndSaveGeneralSubgraphMetrics
import scopt.OptionParser

case class SubgraphMetricsParams(
                                  metric: String = "",
                                  wikidataTriples: String = "",
                                  allSubgraphs: String = "",
                                  topSubgraphItems: String = "",
                                  topSubgraphTriples: String = "",
                                  generalSubgraphMetrics: String = "", //in and out table
                                  eventQueries: String = "",
                                  processedQueries: String = "",
                                  subgraphQueries: String = "",
                                  perSubgraphMetrics: String = "", //out table
                                  subgraphPairMetrics: String = "", //out table
                                  generalQueryMetrics: String = "", //out table
                                  generalSubgraphQueryMetricsPath: String = "", //out table
                                  perSubgraphQueryMetrics: String = "", //out table
                                  subgraphPairQueryMetrics: String = "", //out table
                                  subgraphQItemsMatchInQuery: String = "",
                                  subgraphPredicatesMatchInQuery: String = "",
                                  subgraphUrisMatchInQuery: String = "",
                                  topN: Long = 100,
                                  minItems: Long = 10000
                                )

class SubgraphMetricsLauncher {

  def extractSubgraphMetrics(params: SubgraphMetricsParams): Unit = {

    params.metric match {
      case "general-subgraph-metrics" => extractAndSaveGeneralSubgraphMetrics(
        params.minItems,
        params.wikidataTriples,
        params.allSubgraphs,
        params.topSubgraphItems,
        params.topSubgraphTriples,
        params.generalSubgraphMetrics
      )
      case "detailed-subgraph-metrics" => extractAndSaveDetailedSubgraphMetrics(
        params.topSubgraphItems,
        params.topSubgraphTriples,
        params.generalSubgraphMetrics,
        params.allSubgraphs,
        params.minItems,
        params.perSubgraphMetrics,
        params.subgraphPairMetrics
      )
      case "query-metrics" => extractAndSaveQueryMetrics(
        params.topN,
        params.eventQueries,
        params.processedQueries,
        params.subgraphQueries,
        params.subgraphQItemsMatchInQuery,
        params.subgraphPredicatesMatchInQuery,
        params.subgraphUrisMatchInQuery,
        params.generalQueryMetrics,
        params.generalSubgraphQueryMetricsPath,
        params.perSubgraphQueryMetrics
      )
      case "subgraph-pair-query-metrics" => extractAndSaveSubgraphPairQueryMetrics(params.subgraphQueries, params.subgraphPairQueryMetrics)
      case _ => sys.exit(1)
    }

  }
}

/**
 * Point of entry to extract subgraph metrics. This includes metrics on wikidata subgraphs
 * and on queries that access these subgraphs, also known as `subgraph queries`.
 * This job lists all subgraph metrics and saves parquet files in date format.
 * This job also lists all subgraph query metrics and saves parquet files in daily format.
 *
 * Command line example:
 * spark2-submit --master yarn --driver-memory 2G --executor-memory 16G --executor-cores 8 \
 * --class org.wikidata.query.rdf.spark.metrics.SubgraphMetricsLauncher \
 * --name subgraph-metrics-spark \
 * --queue root.default \
 * general-subgraph-metrics \
 * ~akhatun/rdf-spark-tools-0.3.42-SNAPSHOT-jar-with-dependencies.jar \
 * --wikidata-triples-table discovery.wikibase_rdf/date=20220210/wiki=wikidata \
 * --all-subgraphs-table discovery.table_name/date=20220210/wiki=wikidata \
 * --top-subgraph-items-table discovery.table_name/date=20220210/wiki=wikidata \
 * --top-subgraph-triples-table discovery.table_name/date=20220210/wiki=wikidata \
 * --general-subgraph-metrics-table discovery.table_name/date=20220210/wiki=wikidata \
 * --min-items 10000
 */
object SubgraphMetricsLauncher {

  def main(args: Array[String]): Unit = {
    argParser.parse(args, SubgraphMetricsParams()) match {

      case Some(params) => new SubgraphMetricsLauncher().extractSubgraphMetrics(params)
      case _ => sys.exit(-1)

    }
  }

  // arguments of multiple commands
  // scalastyle:off method.length
  def argParser: OptionParser[SubgraphMetricsParams] = {
    new OptionParser[SubgraphMetricsParams]("") {
      head("Subgraph Metrics Launcher")
      help("help") text "Prints this usage text"

      def allSubgraphsOption = opt[String]("all-subgraphs-table")
        .required()
        .valueName("<table-path>")
        .action { (x, p) => p.copy(allSubgraphs = x) }
        .text("Table holding list of subgraphs and item counts with partition specs")

      def topSubgraphItemsOption = opt[String]("top-subgraph-items-table")
        .required()
        .valueName("<table-path>")
        .action { (x, p) => p.copy(topSubgraphItems = x) }
        .text("Table holding items of top subgraphs (<item> P31 <subgraph>) with partition specs")

      def topSubgraphTriplesOption = opt[String]("top-subgraph-triples-table")
        .required()
        .valueName("<table-path>")
        .action { (x, p) => p.copy(topSubgraphTriples = x) }
        .text("Table holding all triples of top subgraphs with partition specs")

      def subgraphQueryOption = opt[String]("subgraph-query-table")
        .required()
        .valueName("<table-path>")
        .action { (x, p) => p.copy(subgraphQueries = x) }
        .text("Table holding SPARQL query ids mapped to the subgraph it accesses with partition specs")

      def generalSubgraphMetricsOption = opt[String]("general-subgraph-metrics-table")
        .required()
        .valueName("<table-path>")
        .action { (x, p) => p.copy(generalSubgraphMetrics = x) }
        .text("Table holding some basic statistics on wikidata and its subgraphs with partition specs")

      def minItemsOption = opt[Int]("min-items")
        .optional()
        .valueName("<number>")
        .action { (x, p) => p.copy(minItems = x) }
        .text("Number of top entries to save for various analysis. Defaults to 10000")

      cmd("general-subgraph-metrics")
        .action((_, c) => c.copy(metric = "general-subgraph-metrics"))
        .text("general-subgraph-metrics is a command")
        .children(
          minItemsOption,
          opt[String]("wikidata-triples-table")
            .required()
            .valueName("<table-path>")
            .action { (x, p) => p.copy(wikidataTriples = x) }
            .text("Table holding Wikidata snapshots with partition specs"),
          allSubgraphsOption,
          topSubgraphItemsOption,
          topSubgraphTriplesOption,
          generalSubgraphMetricsOption
        )

      cmd("detailed-subgraph-metrics")
        .action((_, c) => c.copy(metric = "detailed-subgraph-metrics"))
        .text("detailed-subgraph-metrics is a command")
        .children(
          minItemsOption,
          generalSubgraphMetricsOption,
          allSubgraphsOption,
          topSubgraphItemsOption,
          topSubgraphTriplesOption,
          opt[String]("per-subgraph-metrics-table")
            .required()
            .valueName("<table-path>")
            .action { (x, p) => p.copy(perSubgraphMetrics = x) }
            .text("Output path to save per subgraph metrics with partition specs."),
          opt[String]("subgraph-pair-metrics-table")
            .required()
            .valueName("<table-path>")
            .action { (x, p) => p.copy(subgraphPairMetrics = x) }
            .text("Output path to save per subgraph-pair metrics with partition specs.")
        )

      cmd("query-metrics")
        .action((_, c) => c.copy(metric = "query-metrics"))
        .text("query-metrics is a command")
        .children(
          opt[String]("event-query-table")
            .required()
            .valueName("<table-path>")
            .action { (x, p) => p.copy(eventQueries = x) }
            .text("Table holding the SPARQL queries with partition specs"),
          opt[String]("processed-query-table")
            .required()
            .valueName("<table-path>")
            .action { (x, p) => p.copy(processedQueries = x) }
            .text("Table holding the parsed SPARQL queries with partition specs"),
          subgraphQueryOption,
          opt[String]("matched-Qitems-table")
            .required()
            .valueName("<table-path>")
            .action { (x, p) => p.copy(subgraphQItemsMatchInQuery = x) }
            .text("Table holding list of Q-items that are contained in each query with partition specs"),
          opt[String]("matched-predicates-table")
            .required()
            .valueName("<table-path>")
            .action { (x, p) => p.copy(subgraphPredicatesMatchInQuery = x) }
            .text("Table holding list of predicates that are contained in each query with partition specs"),
          opt[String]("matched-uris-table")
            .required()
            .valueName("<table-path>")
            .action { (x, p) => p.copy(subgraphUrisMatchInQuery = x) }
            .text("Table holding list of URIs that are contained in each query with partition specs"),
          opt[Int]("top-n")
            .optional()
            .valueName("<number>")
            .action { (x, p) => p.copy(topN = x) }
            .text("Number of top entries to save for various analysis. Defaults to 100"),
          opt[String]("general-query-metrics-table")
            .required()
            .valueName("<table-path>")
            .action { (x, p) => p.copy(generalQueryMetrics = x) }
            .text("Output path to save overall query metrics with partition specs."),
          opt[String]("general-subgraph-query-metrics-table")
            .required()
            .valueName("<table-path>")
            .action { (x, p) => p.copy(generalSubgraphQueryMetricsPath = x) }
            .text("Output path to save overall subgraph query metrics with partition specs."),
          opt[String]("per-subgraph-query-metrics-table")
            .required()
            .valueName("<table-path>")
            .action { (x, p) => p.copy(perSubgraphQueryMetrics = x) }
            .text("Output path to save per subgraph query metrics with partition specs.")
        )

      cmd("subgraph-pair-query-metrics")
        .action((_, c) => c.copy(metric = "subgraph-pair-query-metrics"))
        .text("subgraph-pair-query-metrics is a command")
        .children(
          subgraphQueryOption,
          opt[String]("subgraph-pair-query-metrics-table")
            .required()
            .valueName("<table-path>")
            .action { (x, p) => p.copy(subgraphPairQueryMetrics = x) }
            .text("Output path to save per subgraph-pair query metrics with partition specs.")
        )
    }
  }
  // scalastyle:on method.length
}
