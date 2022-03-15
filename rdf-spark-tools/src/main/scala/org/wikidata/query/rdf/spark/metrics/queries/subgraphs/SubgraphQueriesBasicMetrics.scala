package org.wikidata.query.rdf.spark.metrics.queries.subgraphs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SubgraphQueriesBasicMetrics {

  /** Gets some aggregated query metrics per subgraph.
   *
   * @param totalTime           total query time for all processed queries
   * @param processedQueryCount total number of processed queries
   * @param uaCount             total number of unique user-agents
   * @param subgraphQueriesInfo all subgraphQueries and their info from processedQueries.
   *                            Expected columns: id, subgraph, query, query_time, query_time_class, ua, q_info
   * @return spark dataframe with columns: subgraph, query_count, query_time, ua_count, query_type, percent_query_count,
   *         percent_query_time, percent_ua_count, query_count_rank, query_time_rank, avg_query_time
   */
  def getSubgraphQueriesBasicMetrics(totalTime: Long,
                                     processedQueryCount: Long,
                                     uaCount: Long,
                                     subgraphQueriesInfo: DataFrame): DataFrame = {
    subgraphQueriesInfo
      .groupBy("subgraph")
      .agg(
        countDistinct("id").as("query_count"),
        sum("query_time").as("query_time"),
        countDistinct("ua").as("ua_count"),
        countDistinct("q_info.opList").as("query_type")
      )
      .withColumn("percent_query_count", col("query_count") * 100.0 / processedQueryCount)
      .withColumn("percent_query_time", col("query_time") * 100.0 / totalTime)
      .withColumn("percent_ua_count", col("ua_count") * 100.0 / uaCount)
      .withColumn("query_count_rank", row_number().over(Window.orderBy(desc("query_count"))))
      .withColumn("query_time_rank", row_number().over(Window.orderBy(desc("query_time"))))
      .withColumn("avg_query_time", col("query_time") / col("query_count"))
  }
}
