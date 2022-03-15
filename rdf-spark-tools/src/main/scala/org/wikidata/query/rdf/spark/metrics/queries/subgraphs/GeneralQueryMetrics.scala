package org.wikidata.query.rdf.spark.metrics.queries.subgraphs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.wikidata.query.rdf.spark.utils.SubgraphUtils.sparkDfColumnsToMap

object GeneralQueryMetrics {

  /**
   * Extracts aggregated SPARQL query metrics like total query count, query time, ua count etc.
   *
   * @param eventQueries     existing table on information about queries. Required columns: http.status_code, query.
   *                         Event query table is use to get total query count and the distribution of status code for
   *                         all queries.
   * @param processedQueries parsed SPARQL queries. Expected columns: id, query, query_time, query_time_class, ua, q_info
   * @return tuple of the following items:
   *         - totalTime: Total processed query time
   *         - processedQueryCount : Number of queries processed
   *         - uaCount: Number of distinct user agents
   *         - generalQueryMetrics: spark dataframe with each column containing an aggregated metric on queries
   *           Expected columns: total_query_count, processed_query_count, percent_processed_query, distinct_query_count,
   *           percent_query_repeated, total_ua_count, total_time, status_code_query_count, query_time_class_query_count
   */
  def getGeneralQueryMetrics(eventQueries: DataFrame,
                             processedQueries: DataFrame)(implicit spark: SparkSession): (Long, Long, Long, DataFrame) = {

    val allQueryCount = eventQueries.count()
    val allOkQueryCount = eventQueries
      .filter("http.status_code IN (500, 200)")
      .filter("query != ' ASK{ ?x ?y ?z }'") // these ASK queries are "life-checks" and so removed from analysis
      .count()
    val processedQueryCount = processedQueries.count()
    val percentProcessed = processedQueryCount * 100.0 / allOkQueryCount

    val distinctQueryCount = processedQueries.select("query").distinct().count()
    val percentRepeated = (processedQueryCount - distinctQueryCount) * 100.0 / processedQueryCount

    val queryCountPerStatusCode = sparkDfColumnsToMap(
      eventQueries
        .groupBy("http.status_code")
        .count(),
      "status_code",
      "count",
      "status_code_query_count",
      List()
    )

    val queryTimeClassDistribution = sparkDfColumnsToMap(
      processedQueries
        .groupBy("query_time_class")
        .count(),
      "query_time_class",
      "count",
      "query_time_class_query_count",
      List()
    )

    val uaCount = processedQueries.select("ua").distinct().count()

    val totalTime = processedQueries.groupBy().sum("query_time").first.getLong(0) // in ms

    val data = Seq((allQueryCount, processedQueryCount, percentProcessed, distinctQueryCount,
      percentRepeated, uaCount, totalTime))
    val columns = Seq("total_query_count", "processed_query_count", "percent_processed_query",
      "distinct_query_count", "percent_query_repeated", "total_ua_count", "total_time")

    val generalQueryMetrics = spark.createDataFrame(data)
      .toDF(columns: _*)
      .crossJoin(queryCountPerStatusCode)
      .crossJoin(queryTimeClassDistribution)

    (totalTime, processedQueryCount, uaCount, generalQueryMetrics)
  }
}
