package org.wikidata.query.rdf.spark.metrics.queries.subgraphs

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.wikidata.query.rdf.spark.metrics.queries.subgraphs.SubgraphQueriesBasicMetrics.getSubgraphQueriesBasicMetrics
import org.wikidata.query.rdf.spark.metrics.queries.subgraphs.SubgraphQueryComposition.getSubgraphQueryComposition
import org.wikidata.query.rdf.spark.metrics.queries.subgraphs.SubgraphUaMetrics.getPerSubgraphUaMetrics
import org.wikidata.query.rdf.spark.metrics.queries.subgraphs.TopPathsPerSubgraph.getTopPaths
import org.wikidata.query.rdf.spark.metrics.queries.subgraphs.TopServicesPerSubgraph.getTopServices
import org.wikidata.query.rdf.spark.metrics.queries.subgraphs.TopSubgraphItems.getSubgraphItemsInfo
import org.wikidata.query.rdf.spark.metrics.queries.subgraphs.TopSubgraphProperties.getSubgraphPropertiesInfo
import org.wikidata.query.rdf.spark.metrics.queries.subgraphs.TopSubgraphUris.getSubgraphUrisInfo
import org.wikidata.query.rdf.spark.utils.SubgraphUtils.{sparkDfColumnsToListOfStruct, sparkDfColumnsToMap}

object PerSubgraphQueryMetrics {

  /** Gets per subgraph query metrics.
   * Calls getSubgraphQueriesBasicMetrics, getSubgraphQueryComposition, getPerSubgraphUaMetrics, getTopPaths,
   * getTopServices, getSubgraphItemsInfo, getSubgraphPropertiesInfo, getSubgraphUrisInfo
   *
   * @param totalTime                      total query time for all processed queries
   * @param processedQueryCount            total number of processed queries
   * @param uaCount                        total number of unique user-agents
   * @param numOfSubgraphsPerQuery         Number of Queries that access `X` Number of Subgraphs together.
   *                                       Expected columns: id, subgraph_count
   * @param subgraphQueriesInfo            all subgraphQueries and their info from processedQueries.
   *                                       Expected columns: id, subgraph, query, query_time, query_time_class, ua, q_info
   * @param subgraphQueries                mapping of query to subgraph. Expected columns: id, subgraph, qid, item, predicate, uri, literal
   * @param subgraphQItemsMatchInQuery     List of queries that matched with subgraphs due to an item.
   *                                       Expected columns: id, subgraph, item
   * @param subgraphPredicatesMatchInQuery List of queries that matched with subgraphs due to a predicate.
   *                                       Expected columns: id, subgraph, predicate_code
   * @param subgraphUrisMatchInQuery       List of queries that matched with subgraphs due to a URI.
   *                                       Expected columns: id, subgraph, uri
   * @param topN                           Number of top item, predicates, uris etc to save.
   * @return spark dataframe with columns: subgraph, query_count, query_time, ua_count, query_type, percent_query_count,
   *         percent_query_time, percent_ua_count, query_count_rank, query_time_rank, avg_query_time, qid_count, item_count,
   *         pred_count, uri_count, literal_count, query_time_class_counts, ua_info, ua_query_count_percentiles,
   *         ua_query_count_mean, subgraph_composition, query_only_accessing_this_subgraph, top_items, matched_items_percentiles,
   *         matched_items_mean, top_predicates, matched_predicates_percentiles, matched_predicates_mean, top_uris,
   *         matched_uris_percentiles, matched_uris_mean, service_counts, path_counts
   */

  // method has multiple small pieces, cannot be split further
  // scalastyle:off method.length

  // method also requires all the inputs and are separated for readability
  // scalastyle:off parameter.number
  def getPerSubgraphQueryMetrics(totalTime: Long,
                                 processedQueryCount: Long,
                                 uaCount: Long,
                                 numOfSubgraphsPerQuery: DataFrame,
                                 subgraphQueriesInfo: DataFrame,
                                 subgraphQueries: DataFrame,
                                 subgraphQItemsMatchInQuery: DataFrame,
                                 subgraphPredicatesMatchInQuery: DataFrame,
                                 subgraphUrisMatchInQuery: DataFrame,
                                 topN: Long)(implicit spark: SparkSession): DataFrame = {

    var perSubgraphQueryMetrics = getSubgraphQueriesBasicMetrics(totalTime, processedQueryCount, uaCount, subgraphQueriesInfo)

    val subgraphQueryCompositionCount = getSubgraphQueryComposition(subgraphQueries)

    // Query Time classes
    val queryTimeClass = sparkDfColumnsToMap(
      subgraphQueriesInfo
        .groupBy("subgraph", "query_time_class")
        .count(),
      "query_time_class",
      "count",
      "query_time_class_counts",
      List("subgraph")
    )

    // UA usage distribution
    val (perSubgraphUaMetrics, perSubgraphUAQueryDistribution) = getPerSubgraphUaMetrics(subgraphQueriesInfo, topN, perSubgraphQueryMetrics)

    // Composition of queries in each subgraph
    val subgraphQueryComposition = sparkDfColumnsToListOfStruct(
      subgraphQueries
        .groupBy("subgraph", "item", "predicate", "uri", "qid", "literal")
        .count(),
      List("item", "predicate", "uri", "qid", "literal", "count"),
      "subgraph_composition",
      List("subgraph")
    )

    val queriesOnlyInThisSubgraphsVsWithOthers = numOfSubgraphsPerQuery
      .filter("subgraph_count == 1")
      .join(subgraphQueries, Seq("id"), "left")
      .groupBy("subgraph")
      .count()
      .withColumnRenamed("count", "query_only_accessing_this_subgraph")

    val subgraphWindow = Window.partitionBy("subgraph").orderBy(desc("count"))

    val (topMatchedItems: DataFrame, matchedItemsDistribution: DataFrame) = getSubgraphItemsInfo(subgraphQItemsMatchInQuery, topN, subgraphWindow)
    val (topMatchedPreds: DataFrame, matchedPredsDistribution: DataFrame) = getSubgraphPropertiesInfo(subgraphPredicatesMatchInQuery, topN, subgraphWindow)
    val (topMatchedUris: DataFrame, matchedUrisDistribution: DataFrame) = getSubgraphUrisInfo(subgraphUrisMatchInQuery, topN, subgraphWindow)
    val services: DataFrame = getTopServices(subgraphQueriesInfo)
    val topPaths: DataFrame = getTopPaths(topN, subgraphQueriesInfo, subgraphWindow)

    perSubgraphQueryMetrics = perSubgraphQueryMetrics
      .join(subgraphQueryCompositionCount, Seq("subgraph"), "outer")
      .join(queryTimeClass, Seq("subgraph"), "outer")
      .join(perSubgraphUaMetrics, Seq("subgraph"), "outer")
      .join(perSubgraphUAQueryDistribution, Seq("subgraph"), "outer")
      .join(subgraphQueryComposition, Seq("subgraph"), "outer")
      .join(queriesOnlyInThisSubgraphsVsWithOthers, Seq("subgraph"), "outer")
      .join(topMatchedItems, Seq("subgraph"), "outer")
      .join(matchedItemsDistribution, Seq("subgraph"), "outer")
      .join(topMatchedPreds, Seq("subgraph"), "outer")
      .join(matchedPredsDistribution, Seq("subgraph"), "outer")
      .join(topMatchedUris, Seq("subgraph"), "outer")
      .join(matchedUrisDistribution, Seq("subgraph"), "outer")
      .join(services, Seq("subgraph"), "outer")
      .join(topPaths, Seq("subgraph"), "outer")

    perSubgraphQueryMetrics
  }
  // scalastyle:on parameter.number
  // scalastyle:on method.length
}
