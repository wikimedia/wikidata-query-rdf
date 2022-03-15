package org.wikidata.query.rdf.spark.metrics.queries.subgraphs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions.{col, countDistinct, expr, row_number}
import org.wikidata.query.rdf.spark.utils.SubgraphUtils.{getPercentileExpr, sparkDfColumnsToMap}

object TopSubgraphItems {

  /** Gets the list and distribution of the topN matched items in queries per subgraph.
   *
   * @param subgraphQItemsMatchInQuery List of queries that matched with subgraphs due to an item.
   *                                   Expected columns: id, subgraph, item
   * @param topN                       Number of top matched items to extract.
   * @param subgraphWindow             Window.partitionBy("subgraph").orderBy(desc("count"))
   * @return spark dataframes:
   *         - topMatchedItems: expected columns: subgraph, top_items
   *         - matchedItemsDistribution: expected columns: subgraph, matched_items_percentiles, matched_items_mean
   */
  def getSubgraphItemsInfo(subgraphQItemsMatchInQuery: DataFrame, topN: Long, subgraphWindow: WindowSpec): (DataFrame, DataFrame) = {

    // Top items that caused queries to map to subgraphs and their distribution
    val matchedItemsPerSubgraph = subgraphQItemsMatchInQuery
      .groupBy("subgraph", "item")
      .agg(countDistinct("id").alias("count"))

    // Top items
    val topMatchedItems = sparkDfColumnsToMap(
      matchedItemsPerSubgraph
        .withColumn("rank", row_number().over(subgraphWindow))
        .filter(col("rank") <= topN),
      "item",
      "count",
      "top_items",
      List("subgraph")
    )

    // distribution of item usage in each subgraph
    val matchedItemsDistribution = matchedItemsPerSubgraph
      .groupBy("subgraph")
      .agg(
        expr(getPercentileExpr("count", "matched_items_percentiles")),
        expr("mean(count) as matched_items_mean")
      )
    (topMatchedItems, matchedItemsDistribution)
  }
}
