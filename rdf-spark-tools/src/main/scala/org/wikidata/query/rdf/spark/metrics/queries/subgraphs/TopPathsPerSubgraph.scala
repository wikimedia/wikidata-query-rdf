package org.wikidata.query.rdf.spark.metrics.queries.subgraphs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions.{col, explode, row_number}
import org.wikidata.query.rdf.spark.utils.SubgraphUtils.sparkDfColumnsToMap

object TopPathsPerSubgraph {

  /** Gets the number of times the topN paths are used for queries in each subgraph.
   *
   * @param topN                Number of top paths to extract.
   * @param subgraphQueriesInfo all subgraphQueries and their info from processedQueries.
   *                            Expected columns: id, subgraph, query, query_time, query_time_class, ua, q_info
   * @param subgraphWindow      Window.partitionBy("subgraph").orderBy(desc("count"))
   * @return spark dataframe with columns: subgraph, path_counts: map< string, bigint >
   */
  def getTopPaths(topN: Long, subgraphQueriesInfo: DataFrame, subgraphWindow: WindowSpec): DataFrame = {

    sparkDfColumnsToMap(
      subgraphQueriesInfo
        .select(col("subgraph"), explode(col("q_info.triples")).as("triple"))
        .filter(col("triple.predicateNode.nodeType").startsWith("PATH"))
        .selectExpr("subgraph", "triple.predicateNode.nodeValue as path")
        .groupBy("subgraph", "path")
        .count()
        .withColumn("rank", row_number().over(subgraphWindow))
        .filter(col("rank") <= topN),
      "path",
      "count",
      "path_counts",
      List("subgraph")
    )
  }
}
