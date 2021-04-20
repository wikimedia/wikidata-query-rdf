package org.wikidata.query.rdf.spark.analysis

import org.apache.spark.sql.functions.{col, count, desc, hash, lit, sum, udf}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.Try


/**
 * This class is a wrapper over code run in a notebook,
 * it probably doesn't work as-is.
 */
class QueriesProcessor(spark: SparkSession) {

  val dateClause = "year = 2020 and month = 11"
  val nbParts = 1024

  // UDF getting QueryInfo from query string (or null if it fails)
  val makeQueryInfoUdf = udf((queryString: String) => Try(QueryInfo(queryString)).toOption)
  // UDF getting a subset of a map where keys have the requested prefix (useful for nodes-map parsing)
  val filterMapKeysUdf = udf((map: Map[String, Long], prefix: String) => map.filterKeys(k => k.startsWith(prefix)))

  /**
   * Setup spark to use nbParts parallelism default is SQL mode
   */
  def initSpark: Unit = {
    spark.sql(s"SET spark.sql.shuffle.partitions = $nbParts")
  }

  /**
   * Get query base data from the event table:
   *  - query id
   *  - query text
   *  - query time
   *  - query time class (to facilitate analysis by groups)
   *  - query user-agent
   * @return a spark DataFrame containing the data, explicitly repartitioned to nbPart
   */
  def getBaseData(): DataFrame = {
    spark.sql(s"""
      SELECT
        meta.id as id,
        query,
        query_time,
        CASE
          WHEN query_time < 10 THEN '1_less_10ms'
          WHEN query_time < 100 THEN '2_10ms_to_100ms'
          WHEN query_time < 1000 THEN '3_100ms_to_1s'
          WHEN query_time < 10000 THEN '4_1s_to_10s'
          ELSE '5_more_10s'
        END AS query_time_class,
        http.request_headers['user-agent'] as ua
      FROM event.wdqs_external_sparql_query
      WHERE ${dateClause}
        -- Accepting 500 as they are likely to be timeout
        AND http.status_code IN (500, 200)
        -- Removing monitoring queries
        AND query != ' ASK{ ?x ?y ?z }'
      """).repartition(nbParts)
  }

  def getProcessedQueries(baseData: DataFrame): DataFrame = {
    baseData.
      withColumn("q_info", makeQueryInfoUdf(col("query"))).
      where("q_info IS NOT NULL")
  }

  def getTop100QueryClasses(processedQueries: DataFrame): Array[Row] = {
    processedQueries.
      select(
        col("q_info.opList").alias("l"),
        col("ua"),
        hash(col("ua")).alias("hua"),
        col("query_time"),
        filterMapKeysUdf(col("q_info.nodes"), lit("NODE_VAR")).alias("nodes")).
      // can't aggregate on map, therefore splitting it into two lists (keys, values)
      selectExpr("l", "ua", "hua", "map_keys(nodes) as k", "map_values(nodes) as v").
      groupBy("l", "k", "v", "ua", "hua").
      agg(count(lit(1)).alias("requests"), sum(col("query_time")).alias("sum_query_time")).
      sort(desc("requests")).
      limit(100).
      collect
  }

}

object QueriesProcessor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("triples-analyzer").getOrCreate()
    val processor = new QueriesProcessor(spark)
    val baseData = processor.getBaseData()
    val processedData = processor.getProcessedQueries(baseData)
    // showing 10 lines for demo
    processedData.show(10)
  }
}
