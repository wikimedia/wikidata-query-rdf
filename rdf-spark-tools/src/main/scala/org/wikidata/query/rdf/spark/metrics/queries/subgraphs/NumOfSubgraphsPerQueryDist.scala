package org.wikidata.query.rdf.spark.metrics.queries.subgraphs

import org.apache.spark.sql.DataFrame
import org.wikidata.query.rdf.spark.utils.SubgraphUtils.sparkDfColumnsToListOfStruct

object NumOfSubgraphsPerQueryDist {

  /**
   * @param numOfSubgraphsPerQuery Number of Queries that access `X` Number of Subgraphs.
   *                               Expected columns: id, subgraph_count.
   * @return spark dataframe. Expected columns: id (dummy id for merging purposes),
   *         query_subgraph_dist: struct< subgraph_count, query_count >
   */
  def getNumOfSubgraphsPerQueryDist(numOfSubgraphsPerQuery: DataFrame): DataFrame = {
    sparkDfColumnsToListOfStruct(
      numOfSubgraphsPerQuery
        .groupBy("subgraph_count")
        .count()
        .withColumnRenamed("count", "query_count"),
      List("subgraph_count", "query_count"),
      "query_subgraph_dist",
      List()
    )
  }
}
