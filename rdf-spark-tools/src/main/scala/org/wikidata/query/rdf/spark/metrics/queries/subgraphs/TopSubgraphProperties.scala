package org.wikidata.query.rdf.spark.metrics.queries.subgraphs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions.{col, countDistinct, expr, row_number}
import org.wikidata.query.rdf.spark.utils.SubgraphUtils.{getPercentileExpr, sparkDfColumnsToMap}

object TopSubgraphProperties {

  /** Gets the list and distribution of the topN matched predicates in queries per subgraph.
   *
   * @param subgraphPredicatesMatchInQuery List of queries that matched with subgraphs due to an predicate.
   *                                       Expected columns: id, subgraph, predicate_code
   * @param topN                           Number of top matched predicates to extract.
   * @param subgraphWindow                 Window.partitionBy("subgraph").orderBy(desc("count"))
   * @return spark dataframes:
   *         - topMatchedPreds: expected columns: subgraph, top_predicates
   *         - matchedPredsDistribution: expected columns: subgraph, matched_predicates_percentiles, matched_predicates_mean
   */
  def getSubgraphPropertiesInfo(subgraphPredicatesMatchInQuery: DataFrame, topN: Long, subgraphWindow: WindowSpec): (DataFrame, DataFrame) = {

    // Top matched properties and distribution
    // Add percent of query if necessary (q*100/total_q)
    val matchedPredsPerSubgraph = subgraphPredicatesMatchInQuery
      .groupBy("subgraph", "predicate_code")
      .agg(countDistinct("id").alias("count"))

    // Top prodicates
    val topMatchedPreds = sparkDfColumnsToMap(
      matchedPredsPerSubgraph
        .withColumn("rank", row_number().over(subgraphWindow))
        .filter(col("rank") <= topN),
      "predicate_code",
      "count",
      "top_predicates",
      List("subgraph")
    )

    // distribution of predicate usage in each subgraph
    val matchedPredsDistribution = matchedPredsPerSubgraph
      .groupBy("subgraph")
      .agg(
        expr(getPercentileExpr("count", "matched_predicates_percentiles")),
        expr("mean(count) as matched_predicates_mean")
      )
    (topMatchedPreds, matchedPredsDistribution)
  }
}
