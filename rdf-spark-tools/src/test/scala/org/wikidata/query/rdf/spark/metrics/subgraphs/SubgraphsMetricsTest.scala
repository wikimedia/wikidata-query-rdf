package org.wikidata.query.rdf.spark.metrics.subgraphs

import org.apache.spark.sql.DataFrame
import org.scalatest.Matchers
import org.wikidata.query.rdf.spark.SparkDataFrameComparisons
import org.wikidata.query.rdf.spark.metrics.subgraphs.detailed.DetailedSubgraphMetricsExtractor.getDetailedSubgraphMetrics
import org.wikidata.query.rdf.spark.metrics.subgraphs.general.GeneralSubgraphMetricsExtractor.getGeneralSubgraphMetrics

// These tests are non-exhaustive and the values may not be fully accurate.
// Any correction in the code may result in value changes that should be
// checked for accuracy and updated in the tests below if required. The primary
// aim of these tests is to check that the code runs and gives intended output.
class SubgraphsMetricsTest extends SparkDataFrameComparisons with Matchers {

  var wikidataTriples: DataFrame = _
  var topSubgraphTriples: DataFrame = _
  var topSubgraphItems: DataFrame = _
  var allSubgraphs: DataFrame = _

  "general and detailed subgraph metrics" should "be properly calculated" in {

    // Test generalSubgraphMetrics
    val generalSubgraphMetrics = getGeneralSubgraphMetrics(5, wikidataTriples, allSubgraphs, topSubgraphItems, topSubgraphTriples)

    generalSubgraphMetrics.select("total_items").collect()(0)(0) shouldEqual 23
    generalSubgraphMetrics.select("total_triples").collect()(0)(0) shouldEqual 2545
    generalSubgraphMetrics.select("num_subgraph").collect()(0)(0) shouldEqual 13
    generalSubgraphMetrics.select("num_top_subgraph").collect()(0)(0) shouldEqual 3

    val (perSubgraphMetrics, subgraphPairMetrics) = getDetailedSubgraphMetrics(
      generalSubgraphMetrics, topSubgraphItems, topSubgraphTriples, allSubgraphs, 5
    )

    // scalastyle:off import.grouping
    import spark.implicits._
    // scalastyle:on import.grouping

    // Test perSubgraphMetrics
    val perSubgraphMetricsColumns = List("subgraph", "item_count", "triple_count", "predicate_count", "item_rank", "triple_rank",
      "num_direct_triples", "num_statements", "num_statement_triples", "subgraph_to_WD_triples", "WD_to_subgraph_triples")
    val perSubgraphMetricsValues = perSubgraphMetrics.select(perSubgraphMetricsColumns.head, perSubgraphMetricsColumns.tail: _*)

    val perSubgraphMetricsValuesResultDf = spark.sparkContext.parallelize(
      Seq(
        ("<http://www.wikidata.org/entity/Q11424>", 8, 904, 155, 1, 1, 294, 115, 425, 0, 0),
        ("<http://www.wikidata.org/entity/Q5>", 5, 540, 128, 3, 3, 174, 71, 295, 0, 0),
        ("<http://www.wikidata.org/entity/Q7187>", 6, 636, 77, 2, 2, 210, 81, 345, 0, 0))
    ).toDF(perSubgraphMetricsColumns: _*)

    assertDataFrameDataEqualsImproved(perSubgraphMetricsValues, perSubgraphMetricsValuesResultDf)

    // Test subgraphPairMetrics
    val subgraphPairMetricsColumns = List("subgraph1", "subgraph2", "triples_from_1_to_2", "triples_from_2_to_1", "common_predicate_count", "common_item_count")
    val subgraphPairMetricsValues = subgraphPairMetrics.select(subgraphPairMetricsColumns.head, subgraphPairMetricsColumns.tail: _*)

    val subgraphPairMetricsValuesResultDf = spark.sparkContext.parallelize(
      Seq(
        ("<http://www.wikidata.org/entity/Q11424>", "<http://www.wikidata.org/entity/Q11424>", 7, 0, 0, 0),
        ("<http://www.wikidata.org/entity/Q5>", "<http://www.wikidata.org/entity/Q11424>", 0, 0, 13, 0),
        ("<http://www.wikidata.org/entity/Q7187>", "<http://www.wikidata.org/entity/Q11424>", 0, 0, 12, 0),
        ("<http://www.wikidata.org/entity/Q7187>", "<http://www.wikidata.org/entity/Q5>", 0, 0, 13, 0))
    ).toDF(subgraphPairMetricsColumns: _*)

    assertDataFrameDataEqualsImproved(subgraphPairMetricsValues, subgraphPairMetricsValuesResultDf)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    val subgraphsResourcePath = "/org/wikidata/query/rdf/spark/transform/structureddata/subgraphs/"

    wikidataTriples = spark.read.json(getClass.getResource(subgraphsResourcePath + "wikidata_triples_small.json").toString)
    topSubgraphTriples = spark.read.json(getClass.getResource(subgraphsResourcePath + "top_subgraph_triples.json").toString)
    topSubgraphItems = spark.read.json(getClass.getResource(subgraphsResourcePath + "top_subgraph_items.json").toString)
    allSubgraphs = spark.read.json(getClass.getResource(subgraphsResourcePath + "all_subgraphs.json").toString)
  }
}
