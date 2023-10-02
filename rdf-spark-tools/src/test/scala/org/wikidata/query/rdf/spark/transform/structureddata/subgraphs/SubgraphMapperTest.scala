package org.wikidata.query.rdf.spark.transform.structureddata.subgraphs

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.matchers.should.Matchers
import org.wikidata.query.rdf.spark.SparkDataFrameComparisons

class SubgraphMapperTest extends SparkDataFrameComparisons with Matchers {

  var subgraphMapper: SubgraphMapper = _
  var allSubgraphs: DataFrame = _
  var topSubgraphs: DataFrame = _
  var topSubgraphItems: DataFrame = _
  var topSubgraphTriples: DataFrame = _

  "subgraph mapper" should "identify top subgraphs" in {
    val allSubgraphsTestResult = subgraphMapper.getAllSubgraphs()

    assertDataFrameDataEqualsImproved(allSubgraphsTestResult, allSubgraphs)
  }

  it should "identify all items of the top subgraphs" in {
    val topSubgraphItemsResult = subgraphMapper.getTopSubgraphItems(allSubgraphs, 5)

    assertDataFrameDataEqualsImproved(topSubgraphItemsResult, topSubgraphItems)
  }

  it should "identify all triples of the top subgraphs" in {
    val topSubgraphTriplesResult = subgraphMapper.getTopSubgraphTriples(topSubgraphItems)

    assertDataFrameDataEqualsImproved(topSubgraphTriplesResult, topSubgraphTriples)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    val wikidataTriples = spark.read.json(getClass.getResource("wikidata_triples_small.json").toString)
    subgraphMapper = new SubgraphMapper(wikidataTriples)
    allSubgraphs = spark.read.json(getClass.getResource("all_subgraphs.json").toString)
    topSubgraphs = getTopSubgraphsTestDF
    topSubgraphItems = spark.read.json(getClass.getResource("top_subgraph_items.json").toString)
    topSubgraphTriples = spark.read.json(getClass.getResource("top_subgraph_triples.json").toString)
  }

  def getTopSubgraphsTestDF: DataFrame = {
    val topSubgraphsData = Seq(
      Row("<http://www.wikidata.org/entity/Q5>", 5L),
      Row("<http://www.wikidata.org/entity/Q7187>", 6L),
      Row("<http://www.wikidata.org/entity/Q11424>", 8L)
    )
    val topSubgraphsSchema = StructType(Seq(
      StructField("subgraph", StringType),
      StructField("count", LongType)
    ))
    spark.createDataFrame(spark.sparkContext.parallelize(topSubgraphsData), topSubgraphsSchema)
  }
}
