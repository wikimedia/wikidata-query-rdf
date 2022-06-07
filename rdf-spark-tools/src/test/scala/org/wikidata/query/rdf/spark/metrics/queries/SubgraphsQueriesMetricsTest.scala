package org.wikidata.query.rdf.spark.metrics.queries

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.Matchers
import org.wikidata.query.rdf.spark.SparkDataFrameComparisons
import org.wikidata.query.rdf.spark.metrics.queries.subgraphpairs.SubgraphPairQueryMetricsExtractor.getSubgraphPairQueryMetrics
import org.wikidata.query.rdf.spark.metrics.queries.subgraphs.QueryMetricsExtractor.getQueryMetrics

import scala.io.Source

// These tests are non-exhaustive and the values may not be fully accurate.
// Any correction in the code may result in value changes that should be
// checked for accuracy and updated in the tests below if required. The primary
// aim of these tests is to check that the code runs and gives intended output.

case class uaSubgraphDist(subgraph_count: Long, ua_count: Long)

case class querySubgraphDist(subgraph_count: Long, query_count: Long)

case class queryTimeClassSubgraphDist(subgraph_count: String, query_time_class: Map[String, Long])

class SubgraphsQueriesMetricsTest extends SparkDataFrameComparisons with Matchers {

  var eventQueries: DataFrame = _
  var processedQueries: DataFrame = _
  var subgraphQueries: DataFrame = _
  var subgraphQItemsMatchInQuery: DataFrame = _
  var subgraphPredicatesMatchInQuery: DataFrame = _
  var subgraphUrisMatchInQuery: DataFrame = _

  "general query metrics and subgraph query metrics" should "be properly calculated" in {

    val (generalQueryMetrics, generalSubgraphQueryMetrics, perSubgraphQueryMetrics) = getQueryMetrics(
      10,
      eventQueries,
      processedQueries,
      subgraphQueries,
      subgraphQItemsMatchInQuery,
      subgraphPredicatesMatchInQuery,
      subgraphUrisMatchInQuery
    )

    // scalastyle:off import.grouping
    import spark.implicits._
    // scalastyle:on import.grouping

    // Test generalQueryMetrics
    val generalQueryMetricsColumns = List("total_query_count", "processed_query_count", "distinct_query_count",
      "total_ua_count", "total_time", "status_code_query_count", "query_time_class_query_count")
    val generalQueryMetricsValues = generalQueryMetrics.select(generalQueryMetricsColumns.head, generalQueryMetricsColumns.tail: _*)

    val generalQueryMetricsValuesResultDf = spark.sparkContext.parallelize(
      Seq((
        39, 22, 21, 17, 188452, Map(500 -> 2, 403 -> 10, 200 -> 27),
        Map("2_10ms_to_100ms" -> 12, "3_100ms_to_1s" -> 4, "4_1s_to_10s" -> 3, "5_more_10s" -> 3)
      ))
    ).toDF(generalQueryMetricsColumns: _*)

    assertDataFrameDataEqualsImproved(generalQueryMetricsValues, generalQueryMetricsValuesResultDf)

    // Test generalSubgraphQueryMetrics
    val generalSubgraphQueryMetricsValues = generalSubgraphQueryMetrics
      .select(col("total_subgraph_query_count"), explode(col("query_time_class_subgraph_dist")))
      .select("total_subgraph_query_count", "col.subgraph_count", "col.query_time_class")

    val generalSubgraphQueryMetricsValuesResultDf = spark.sparkContext.parallelize(
      Seq(
        (19, "n/a", Map("2_10ms_to_100ms" -> 3)),
        (19, "1", Map("2_10ms_to_100ms" -> 9, "3_100ms_to_1s" -> 2, "4_1s_to_10s" -> 2, "5_more_10s" -> 2)),
        (19, "2", Map("3_100ms_to_1s" -> 4, "4_1s_to_10s" -> 2, "5_more_10s" -> 2))
      )
    ).toDF("total_subgraph_query_count", "subgraph_count", "query_time_class")

    assertDataFrameDataEqualsImproved(generalSubgraphQueryMetricsValues, generalSubgraphQueryMetricsValuesResultDf)

    // Test perSubgraphQueryMetrics
    val perSubgraphQueryMetricsColumns = List("subgraph", "query_count", "query_time", "ua_count", "query_type",
      "query_count_rank", "query_time_rank", "qid_count", "item_count", "pred_count", "uri_count", "literal_count",
      "query_only_accessing_this_subgraph", "query_time_class_counts")
    val perSubgraphQueryMetricsValues = perSubgraphQueryMetrics.select(perSubgraphQueryMetricsColumns.head, perSubgraphQueryMetricsColumns.tail: _*)

    val perSubgraphQueryMetricsValuesResultDf = spark.sparkContext.parallelize(
      Seq(
        ("<http://www.wikidata.org/entity/Q5>", 10, 64998, 10, 10, 1, 2, 4, 1, 6, 4, 2, 6,
          Map("3_100ms_to_1s" -> 2, "4_1s_to_10s" -> 2, "5_more_10s" -> 1, "2_10ms_to_100ms" -> 5)),
        ("<http://www.wikidata.org/entity/Q7187>", 4, 508, 4, 3, 3, 3, 3, 0, 2, 3, 1, 3,
          Map("3_100ms_to_1s" -> 1, "2_10ms_to_100ms" -> 3)),
        ("<http://www.wikidata.org/entity/Q11424>", 9, 185068, 6, 8, 2, 1, 3, 1, 6, 7, 4, 6,
          Map("3_100ms_to_1s" -> 3, "4_1s_to_10s" -> 2, "2_10ms_to_100ms" -> 1, "5_more_10s" -> 3))
      )
    ).toDF(perSubgraphQueryMetricsColumns: _*)

    assertDataFrameDataEqualsImproved(perSubgraphQueryMetricsValues, perSubgraphQueryMetricsValuesResultDf)
  }

  "subgraph pair query metrics" should "be properly calculated" in {
    val queriesPerSubgraphPair = getSubgraphPairQueryMetrics(subgraphQueries)

    // scalastyle:off import.grouping
    import spark.implicits._
    // scalastyle:on import.grouping

    val queriesPerSubgraphPairResultDf = spark.sparkContext.parallelize(
      Seq(
        ("<http://www.wikidata.org/entity/Q7187>", "<http://www.wikidata.org/entity/Q5>", 1),
        ("<http://www.wikidata.org/entity/Q5>", "<http://www.wikidata.org/entity/Q11424>", 3))
    ).toDF(queriesPerSubgraphPair.columns: _*)

    assertDataFrameDataEqualsImproved(queriesPerSubgraphPair, queriesPerSubgraphPairResultDf)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    val queriesResourcePath = "/org/wikidata/query/rdf/spark/transform/queries/subgraphsqueries/"

    val schemaSource = Source.fromURL(getClass.getResource(queriesResourcePath + "queries_small_sample_schema.json"))
    val schema = DataType.fromJson(schemaSource.getLines.mkString).asInstanceOf[StructType]
    processedQueries = spark.read.schema(schema).json(getClass.getResource(queriesResourcePath + "queries_small_sample.json").toString)
    schemaSource.close()

    subgraphQueries = spark.read.json(getClass.getResource(queriesResourcePath + "subgraphs_queries_mapping.json").toString)

    eventQueries = spark.read.json(getClass.getResource("event_queries.json").toString)
    subgraphQItemsMatchInQuery = spark.read.json(getClass.getResource("subgraph_qItems_match_in_query.json").toString)
    subgraphPredicatesMatchInQuery = spark.read.json(getClass.getResource("subgraph_predicates_match_in_query.json").toString)
    subgraphUrisMatchInQuery = spark.read.json(getClass.getResource("subgraph_uris_match_in_query.json").toString)
  }
}
