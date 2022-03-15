package org.wikidata.query.rdf.spark.metrics.queries.subgraphs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.wikidata.query.rdf.spark.utils.SubgraphUtils.{getPercentileExpr, sparkDfColumnsToListOfStruct}

object SubgraphUaMetrics {

  /** Gets per subgraph topN user-agent list and corresponding metrics.
   *
   * @param subgraphQueriesInfo     all subgraphQueries and their info from processedQueries.
   *                                Expected columns: id, subgraph, query, query_time, query_time_class, ua, q_info
   * @param topN                    Number of top user-agents' information to extract.
   * @param perSubgraphQueryMetrics spark dataframe with columns: subgraph, query_count, query_time, ua_count, query_type, percent_query_count,
   *                                percent_query_time, percent_ua_count, query_count_rank, query_time_rank, avg_query_time
   * @return spark dataframe for user-agent metrics with columns:
   *         - subgraph
   *         - ua_info: array< struct<
   *           ua_rank, ua_query_count, ua_query_time, ua_query_type, ua_query_percent,
   *           ua_query_time_percent, ua_avg_query_time, ua_query_type_percent> >
   * @return spark dataframe with aggregate user-agent query count stats with columns:
   *         - ua_query_count_percentiles
   *         - ua_query_count_mean
   */
  def getPerSubgraphUaMetrics(subgraphQueriesInfo: DataFrame, topN: Long, perSubgraphQueryMetrics: DataFrame): (DataFrame, DataFrame) = {

    // distinct done here to make sure query-time-sum is done for distinct queries
    var perSubgraphUaMetrics = subgraphQueriesInfo
      .dropDuplicates("subgraph", "ua", "id")
      .groupBy("subgraph", "ua")
      .agg(
        count("id").as("ua_query_count"),
        sum("query_time").as("ua_query_time"),
        countDistinct("q_info.opList").as("ua_query_type")
      )
      .join(
        perSubgraphQueryMetrics.select("subgraph", "query_count", "query_time", "query_type"),
        Seq("subgraph"),
        "inner"
      )
      .withColumn("ua_query_percent", col("ua_query_count") * 100.0 / col("query_count"))
      .withColumn("ua_query_time_percent", col("ua_query_time") * 100.0 / col("query_time"))
      .withColumn("ua_avg_query_time", col("ua_query_time") / col("ua_query_count"))
      .withColumn("ua_query_type_percent", col("ua_query_type") * 100.0 / col("query_type"))
      .drop("query_count", "query_time", "query_type")

    val perSubgraphUAQueryDistribution = perSubgraphUaMetrics
      .groupBy("subgraph")
      .agg(
        expr(getPercentileExpr("ua_query_count", "ua_query_count_percentiles")),
        expr("mean(ua_query_count) as ua_query_count_mean")
      )

    val subgraphWindow = Window.partitionBy("subgraph").orderBy(desc("ua_query_count"))

    perSubgraphUaMetrics = sparkDfColumnsToListOfStruct(
      perSubgraphUaMetrics
        .withColumn("ua_rank", row_number().over(subgraphWindow))
        .filter(col("ua_rank") <= topN),
      List("ua_rank", "ua_query_count", "ua_query_time", "ua_query_type", "ua_query_percent",
        "ua_query_time_percent", "ua_avg_query_time", "ua_query_type_percent"),
      "ua_info",
      List("subgraph")
    )

    (perSubgraphUaMetrics, perSubgraphUAQueryDistribution)
  }
}
