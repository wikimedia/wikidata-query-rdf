package org.wikidata.query.rdf.spark.metrics.queries.subgraphs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.countDistinct
import org.wikidata.query.rdf.spark.metrics.queries.subgraphs.NumOfSubgraphsPerQueryDist.getNumOfSubgraphsPerQueryDist
import org.wikidata.query.rdf.spark.metrics.queries.subgraphs.QueryTimeClassBySubgraphDist.getQueryTimeClassBySubgraphDist
import org.wikidata.query.rdf.spark.metrics.queries.subgraphs.UaSubgraphCounts.getUaSubgraphCounts

object GeneralSubgraphQueryMetrics {

  /**
   * Extracts aggregated subgraphs query metrics like distribution of user-agent count, query count etc.
   * Calls getUaSubgraphCounts, getNumOfSubgraphsPerQueryDist, and getQueryTimeClassBySubgraphDist methods
   *
   * @param processedQueries parsed SPARQL queries. Expected columns: id, query, query_time, query_time_class, ua, q_info
   * @param subgraphQueries  mapping of query to subgraph. Expected columns: id, subgraph, qid, item, predicate, uri, literal
   * @return tuple of the following dataframes:
   *         - numOfSubgraphsPerQuery: Number of subgraphs each query accesses
   *           Expected columns: id, subgraph_count
   *         - subgraphQueriesInfo: all subgraphQueries (queries mapped to subgraphs) and their info from processedQueries
   *           Expected columns: id, subgraph, query, query_time, query_time_class, ua, q_info
   *         - generalSubgraphQueryMetrics: spark dataframe with each column containing an aggregated metric on subgraph queries
   *           Expected columns: total_subgraph_query_count, ua_subgraph_dist, query_subgraph_dist, query_time_class_subgraph_dist
   */
  def getGeneralSubgraphQueryMetrics(processedQueries: DataFrame,
                                     subgraphQueries: DataFrame)(implicit spark: SparkSession): (DataFrame, DataFrame, DataFrame) = {

    val subgraphQueryCount = subgraphQueries.select("id").distinct().count()

    val subgraphQueriesInfo = subgraphQueries.join(processedQueries, Seq("id"), "left")

    // UA vs Number of subgraphs accessed (How many UA access how many subgraphs?)
    val uaSubgraphCounts = getUaSubgraphCounts(subgraphQueriesInfo)

    // Number of Queries that access `X` Number of Subgraphs together.
    // This gives: How many Queries use only 1 subgraph, how many use 2, more than 2 etc
    val numOfSubgraphsPerQuery = subgraphQueries
      .groupBy("id")
      .agg(countDistinct("subgraph").as("subgraph_count"))

    val numOfSubgraphsPerQueryDist = getNumOfSubgraphsPerQueryDist(numOfSubgraphsPerQuery)

    val queryTimeClassBySubgraphAccess: DataFrame = getQueryTimeClassBySubgraphDist(
      subgraphQueries,
      processedQueries,
      subgraphQueriesInfo,
      numOfSubgraphsPerQuery)

    val generalSubgraphQueryMetrics = spark.createDataFrame(Seq(Tuple1(subgraphQueryCount))).toDF("total_subgraph_query_count")
      .crossJoin(uaSubgraphCounts)
      .crossJoin(numOfSubgraphsPerQueryDist)
      .crossJoin(queryTimeClassBySubgraphAccess)

    (numOfSubgraphsPerQuery, subgraphQueriesInfo, generalSubgraphQueryMetrics)
  }
}
