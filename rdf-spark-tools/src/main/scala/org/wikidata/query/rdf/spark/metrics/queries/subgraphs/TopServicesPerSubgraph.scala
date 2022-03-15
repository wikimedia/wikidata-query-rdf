package org.wikidata.query.rdf.spark.metrics.queries.subgraphs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode, lit}
import org.wikidata.query.rdf.spark.utils.SubgraphUtils.{extractItem, sparkDfColumnsToMap}

object TopServicesPerSubgraph {

  /** Gets the number of times various services are used for queries in each subgraph.
   *
   * @param subgraphQueriesInfo all subgraphQueries and their info from processedQueries.
   *                            Expected columns: id, subgraph, query, query_time, query_time_class, ua, q_info
   * @return spark dataframe with columns: subgraph, service_counts: map< string, bigint >
   */
  def getTopServices(subgraphQueriesInfo: DataFrame): DataFrame = {

    sparkDfColumnsToMap(
      subgraphQueriesInfo
        .select(col("subgraph"), explode(col("q_info.services")))
        .select(col("subgraph"), extractItem(col("key"), lit("NODE_URI")).alias("service"))
        .groupBy("subgraph", "service")
        .count(),
      "service",
      "count",
      "service_counts",
      List("subgraph")
    )
  }
}
