package org.wikidata.query.rdf.spark.metrics.queries.subgraphs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{count, countDistinct}
import org.wikidata.query.rdf.spark.utils.SubgraphUtils.sparkDfColumnsToListOfStruct

object UaSubgraphCounts {

  /**
   * @param subgraphQueriesInfo all subgraphQueries (queries mapped to subgraphs) and their info from processedQueries.
   *                            Expected columns: id, subgraph, query, query_time, query_time_class, ua, q_info
   * @return spark dataframe with columns: id (dummy id for merging purposes), ua_subgraph_dist: struct< subgraph_count, ua_count >
   */
  def getUaSubgraphCounts(subgraphQueriesInfo: DataFrame): DataFrame = {
    sparkDfColumnsToListOfStruct(subgraphQueriesInfo
      .groupBy("ua")
      .agg(countDistinct("subgraph").as("subgraph_count"))
      .groupBy("subgraph_count")
      .agg(count("ua").as("ua_count")),
      List("subgraph_count", "ua_count"),
      "ua_subgraph_dist",
      List()
    )
  }
}
