package org.wikidata.query.rdf.spark.analysis

import org.apache.spark.sql.functions.{col, count, desc, hash, lit, sum, udf}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.SaveMode.Overwrite
import org.wikidata.query.rdf.spark.SparkUtils

class QueriesProcessor(tableAndPartitionSpec: String, numPartitions: Int)(implicit spark: SparkSession) {

  // UDF getting QueryInfo from query string (or null if it fails)
  val makeQueryInfoUdf = udf((queryString: String) => QueryInfo(queryString))
  // UDF getting a subset of a map where keys have the requested prefix (useful for nodes-map parsing)
  val filterMapKeysUdf = udf((map: Map[String, Long], prefix: String) => map.filterKeys(k => k.startsWith(prefix)))

  /**
   * Setup spark to use numPartitions parallelism default is SQL mode
   */
  def initSpark: Unit = {
    spark.sql(s"SET spark.sql.shuffle.partitions = $numPartitions")
  }

  /**
   * Get query base data from the event table:
   *  - query id
   *  - query text
   *  - query time
   *  - query time class (to facilitate analysis by groups)
   *  - query user-agent
   * @return a spark DataFrame containing the data, explicitly repartitioned to numPartitions
   */
  def getBaseData(): DataFrame = {
    SparkUtils.readTablePartition(tableAndPartitionSpec)
      .selectExpr(
        "meta.id as id",
        "query",
        "query_time",
        """
        CASE
          WHEN query_time < 10 THEN '1_less_10ms'
          WHEN query_time < 100 THEN '2_10ms_to_100ms'
          WHEN query_time < 1000 THEN '3_100ms_to_1s'
          WHEN query_time < 10000 THEN '4_1s_to_10s'
          ELSE '5_more_10s'
          END AS query_time_class
        """,
        "http.request_headers['user-agent'] as ua"
        )
      //Accepting 500 as they are likely to be timeout
      .filter("http.status_code IN (500, 200)")
      //Removing monitoring queries
      .filter("query != ' ASK{ ?x ?y ?z }'")
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

  def sparkSession: SparkSession = {
    SparkSession
      .builder()
      // required because spark would fail with:
      // Exception in thread "main" org.apache.spark.SparkException: Dynamic partition strict mode requires
      // at least one static partition column. To turn this off set // hive.exec.dynamic.partition.mode=nonstrict
      .config("hive.exec.dynamic.partition", value = true)
      .config("hive.exec.dynamic.partition.mode", "non-strict")
      // Allows overwriting the target partitions
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .appName("SparqlExtractor")
      .getOrCreate()
  }

  def getSparqlDf(tableAndPartitionSpec: String, numPartitions: Int)(implicit spark: SparkSession): DataFrame = {
    val processor = new QueriesProcessor(tableAndPartitionSpec, numPartitions)
    val baseData = processor.getBaseData()
    val processedData = processor.getProcessedQueries(baseData)
    processedData
  }

  def extractAndSaveQuery(params: Params): Unit = {
    implicit val spark: SparkSession = sparkSession

    // Get the data
    val data = getSparqlDf(params.inputTable, params.numPartitions)

    // Save the data
    SparkUtils.insertIntoTablePartition(params.outputTable,
                                        data,
                                        saveMode = Overwrite,
                                        format = Some("hive"))
  }
}
