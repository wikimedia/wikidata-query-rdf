package org.wikidata.query.rdf.spark.metrics.queries.subgraphs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions.{col, countDistinct, expr, row_number}
import org.wikidata.query.rdf.spark.utils.SubgraphUtils.{getPercentileExpr, sparkDfColumnsToMap}

object TopSubgraphUris {

  /** Gets the list and distribution of the topN matched URIs in queries per subgraph.
   *
   * @param subgraphUrisMatchInQuery List of queries that matched with subgraphs due to a URI.
   *                                 Expected columns: id, subgraph, uri
   * @param topN                     Number of top matched items to extract.
   * @param subgraphWindow           Window.partitionBy("subgraph").orderBy(desc("count"))
   * @return spark dataframes:
   *         - topMatchedUris: expected columns: subgraph, top_uris
   *         - matchedUrisDistribution: expected columns: subgraph, matched_uris_percentiles, matched_uris_mean
   */
  def getSubgraphUrisInfo(subgraphUrisMatchInQuery: DataFrame, topN: Long, subgraphWindow: WindowSpec): (DataFrame, DataFrame) = {
    // Top URIs and distribution matched
    // Add percent of query if necessary (q*100/total_q)
    val matchedUrisPerSubgraph = subgraphUrisMatchInQuery
      .groupBy("subgraph", "uri")
      .agg(countDistinct("id").alias("count"))

    // Top URIs
    val topMatchedUris = sparkDfColumnsToMap(
      matchedUrisPerSubgraph
        .withColumn("rank", row_number().over(subgraphWindow))
        .filter(col("rank") <= topN),
      "uri",
      "count",
      "top_uris",
      List("subgraph")
    )

    // distribution of URI usage in each subgraph
    val matchedUrisDistribution = matchedUrisPerSubgraph
      .groupBy("subgraph")
      .agg(
        expr(getPercentileExpr("count", "matched_uris_percentiles")),
        expr("mean(count) as matched_uris_mean")
      )
    (topMatchedUris, matchedUrisDistribution)
  }
}
